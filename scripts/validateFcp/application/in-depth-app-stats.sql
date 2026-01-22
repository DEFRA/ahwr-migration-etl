WITH app_types AS (
    SELECT DISTINCT type
    FROM application
    WHERE type IN ('EE', 'VV')
),

nd AS (
    SELECT
        type,
        COUNT(*) FILTER (WHERE data IS NULL)     AS null_data,
        COUNT(*) FILTER (WHERE data IS NOT NULL) AS non_null_data
    FROM application
    WHERE type IN ('EE', 'VV')
    GROUP BY type
),

keys AS (
    SELECT
        a.type,
        key,
        jsonb_typeof(a.data -> key) AS key_type
    FROM application a
    CROSS JOIN LATERAL jsonb_object_keys(a.data) AS key
    WHERE a.type IN ('EE', 'VV')
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
    FROM application
    WHERE type IN ('EE', 'VV')
      AND (
          -- root-level fields
--           NOT (data ? 'type') OR
          NOT (data ? 'reference') OR
          NOT (data ? 'declaration') OR
          NOT (data ? 'offerStatus') OR
--           NOT (data ? 'organisation') OR
          NOT (data ? 'confirmCheckDetails') OR
          -- organisation-level fields
          NOT (data -> 'organisation' ? 'crn') OR
          NOT (data -> 'organisation' ? 'sbi') OR
          NOT (data -> 'organisation' ? 'name') OR
          NOT (data -> 'organisation' ? 'email') OR
          NOT (data -> 'organisation' ? 'address') OR
          NOT (data -> 'organisation' ? 'orgEmail') OR
          NOT (data -> 'organisation' ? 'userType') OR
          NOT (data -> 'organisation' ? 'farmerName')
      )
    GROUP BY type
)

SELECT jsonb_agg(
    jsonb_build_object(
        'application_type', application_type,
        'metrics', result
    )
) from (
SELECT
    t.type AS application_type,
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
FROM app_types t
JOIN nd ON nd.type = t.type
JOIN missings m ON m.type = t.type
ORDER BY t.type) s;

select json_agg(z) from
(WITH apps AS (
    SELECT
        type,
        jsonb_build_object(
            'duplicates', 0,
            'total_rows', COUNT(*),
            'first_created_application', MIN("createdAt"),
            'last_created_application', MAX("createdAt"),
            'avg_ts_epoch_created', AVG(EXTRACT(EPOCH FROM "createdAt")),
            'first_updated_application', MIN("updatedAt"),
            'last_updated_application', MAX("updatedAt"),
            'avg_ts_epoch_updated', AVG(EXTRACT(EPOCH FROM "updatedAt")),
            'reference_length', jsonb_build_object(
                'min', MIN(LENGTH("reference")),
                'max', MAX(LENGTH("reference")),
                'avg', AVG(LENGTH("reference"))
            ),
            'invalid_update_date', COUNT(*) FILTER (WHERE "updatedAt" < "createdAt"),
            'claimed', jsonb_build_object(
                'true', COUNT(*) FILTER (WHERE claimed = TRUE),
                'false', COUNT(*) FILTER (WHERE claimed = FALSE)
            ),
            'eligible_pii_redaction', jsonb_build_object(
                'true', COUNT(*) FILTER (WHERE "eligiblePiiRedaction" = TRUE),
                'false', COUNT(*) FILTER (WHERE "eligiblePiiRedaction" = FALSE)
            )
        ) AS app_metrics
    FROM application
    WHERE type IN ('EE', 'VV')
    GROUP BY type
)
SELECT * FROM apps) z;


