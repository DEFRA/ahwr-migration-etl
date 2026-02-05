select json_agg(z) from
                       (WITH base AS (
                           SELECT
                               m.*,
                               m.data AS d0
                           FROM message_log m
                       ),
                             step1 AS (
                                 -- Rename commsAddress → commsAddresses
                                 SELECT
                                     *,
                                     CASE
                                         WHEN d0 #> '{outboundMessage,data,commsAddress}' IS NOT NULL
                            THEN jsonb_set(
                            d0 #- '{outboundMessage,data,commsAddress}',
                            '{outboundMessage,data,commsAddresses}',
                            (d0 #> '{outboundMessage,data,commsAddress}')
                            )
                            ELSE d0
                            END AS d1
                        FROM base
                       ),
                       step2 AS (
-- Replace inboundMessage customParams link_to_file.file
SELECT
    *,
    CASE
        WHEN d1 #> '{inboundMessage,customParams,link_to_file,file}' IS NOT NULL
    THEN jsonb_set(
                    d1,
                    '{inboundMessage,customParams,link_to_file,file}',
                    '"Base64NotMigrated"'::jsonb,
                    false
                )
            ELSE d1
END AS d2
    FROM step1
),
step3 AS (
    -- Replace outboundMessage personalisation link_to_file.file
    SELECT
        *,
        CASE
            WHEN d2 #> '{outboundMessage,data,personalisation,link_to_file,file}' IS NOT NULL
            THEN jsonb_set(
                    d2,
                    '{outboundMessage,data,personalisation,link_to_file,file}',
                    '"Base64NotMigrated"'::jsonb,
                    false
                )
            ELSE d2
        END AS d3
    FROM step2
)
SELECT
    "agreementReference",
    "claimReference",
    jsonb_build_object(
            '$date',
            to_char("createdAt" AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
    ) AS "createdAt",
    jsonb_build_object(
            '$date',
            to_char("updatedAt" AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
    ) AS "updatedAt",
    jsonb_strip_nulls(jsonb_build_object('legacyId', id)) AS "legacyData",
    "templateId",
    status,
    true as "migratedRecord",
    jsonb_strip_nulls(d3) AS data
FROM step3) z;