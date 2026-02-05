select json_agg(z) from
    (SELECT m."agreementReference",
            m."claimReference",
            replace(m."messageType", '-5', '-IN_CHECK') as "messageType",
            jsonb_strip_nulls(m.data) AS "data",
            jsonb_strip_nulls(jsonb_build_object('legacyId', m.id)) AS "legacyData",
            true as "migratedRecord",
            jsonb_build_object('$date', TO_CHAR(m."createdAt" AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')) AS "createdAt",
            jsonb_build_object('$date', TO_CHAR(m."updatedAt" AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')) AS "updatedAt"
     FROM message_generate m
     ORDER BY m."createdAt") z;