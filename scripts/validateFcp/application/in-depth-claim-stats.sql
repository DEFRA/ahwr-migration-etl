

WITH claim_types AS (
    SELECT DISTINCT type
    FROM claim
    WHERE type IN ('R', 'E')
),

     nd AS (
         SELECT
             type,
             COUNT(*) FILTER (WHERE data IS NULL)     AS null_data,
             COUNT(*) FILTER (WHERE data IS NOT NULL) AS non_null_data
         FROM claim
         WHERE type IN ('R', 'E')
         GROUP BY type
     ),

     keys AS (
         SELECT
             c.type,
             key,
             jsonb_typeof(c.data -> key) AS key_type
         FROM claim c
                  CROSS JOIN LATERAL jsonb_object_keys(c.data) AS key
         WHERE c.type IN ('R', 'E')
     ),

     ko AS (
         SELECT
             type,
             key,
             key_type,
             COUNT(*) AS occurrences
         FROM keys
         GROUP BY type, key, key_type
     ),

     missings AS (
         SELECT
             type,
             COUNT(*) AS missing_json_payload_fields_count
         FROM claim
         WHERE type IN ('R', 'E')
           AND (
             NOT (data ? 'amount') OR
             NOT (data ? 'vetsName') OR
             NOT (data ? 'claimType') OR
             NOT (data ? 'dateOfVisit') OR
             NOT (data ? 'testResults') OR
             NOT (data ? 'dateOfTesting') OR
             NOT (data ? 'laboratoryURN') OR
             NOT (data ? 'vetRCVSNumber') OR
             NOT (data ? 'typeOfLivestock') OR
             NOT (data ? 'reviewTestResults')
             )
         GROUP BY type
     )

SELECT jsonb_agg(
               jsonb_build_object(
                       'claim_type', claim_type,
                       'metrics', result
               )
       ) from (
                  SELECT
                      t.type AS claim_type,
                      jsonb_build_object(
                              'data_nullability', jsonb_build_object(
                              'null_data', nd.null_data,
                              'non_null_data', nd.non_null_data
                                                  ),
                              'json_key_statistics', (
                                  SELECT jsonb_agg(
                                                 jsonb_build_object(
                                                         'key', ko.key,
                                                         'type', ko.key_type,
                                                         'occurrences', ko.occurrences
                                                 )
                                                 ORDER BY ko.occurrences DESC
                                         )
                                  FROM ko
                                  WHERE ko.type = t.type
                              ),
                              'missing_payload_fields', m.missing_json_payload_fields_count
                      ) AS result
                  FROM claim_types t
                           JOIN nd ON nd.type = t.type
                           JOIN missings m ON m.type = t.type
                  ORDER BY t.type) s;

WITH claims AS (
    SELECT
        jsonb_build_object(
            'duplicates', 0,
            'total_rows', COUNT(*),
            'total_reviews', COUNT(*) FILTER (WHERE "type" = 'R'),
            'total_follow_ups', COUNT(*) FILTER (WHERE "type" = 'E'),
            'total_in_check', COUNT(*) FILTER (WHERE "statusId" = 5),
            'total_on_hold', COUNT(*) FILTER (WHERE "statusId" = 11),
            'total_recc_pay', COUNT(*) FILTER (WHERE "statusId" = 12),
            'total_recc_reject', COUNT(*) FILTER (WHERE "statusId" = 13),
            'total_rejected', COUNT(*) FILTER (WHERE "statusId" = 10),
            'total_ready_to_pay', COUNT(*) FILTER (WHERE "statusId" = 9),
            'total_paid', COUNT(*) FILTER (WHERE "statusId" = 8),
            'total_other_statuses', COUNT(*) FILTER (WHERE "statusId" not in (5,8,9,10,11,12,13)),
            'first_created_claim', MIN("createdAt"),
            'last_created_claim', MAX("createdAt"),
            'avg_ts_epoch_created', AVG(EXTRACT(EPOCH FROM "createdAt")),
            'first_updated_claim', MIN("updatedAt"),
            'last_updated_claim', MAX("updatedAt"),
            'avg_ts_epoch_updated', AVG(EXTRACT(EPOCH FROM "updatedAt")),
            'reference_length', jsonb_build_object(
                'min', MIN(LENGTH("reference")),
                'max', MAX(LENGTH("reference")),
                'avg', AVG(LENGTH("reference"))
            ),
            'invalid_update_date', COUNT(*) FILTER (WHERE "updatedAt" < "createdAt"),
            'invalid_claim_amount', COUNT(*) FILTER (WHERE data-> 'amount' is null OR jsonb_typeof(data->'amount') <> 'number')
        ) AS claim_metrics
    FROM claim
)
SELECT * FROM claims;

