select json_agg(z) from
    (select d.reference,
            jsonb_build_object('$date', TO_CHAR(d."createdAt" AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')) as "createdAt",
            jsonb_build_object('$date', TO_CHAR(d."updatedAt" AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')) as "updatedAt",
            jsonb_strip_nulls(
                    jsonb_build_object(
                            'emailReference', d."emailReference",
                            'completed', d.completed,
                            'legacyId', d.id
                    )
            ) AS "legacyData",
            d."fileName",
            d.status,
            true as "migratedRecord",
            jsonb_strip_nulls(
                    d.data
            ) AS "inputData"
     from document_log d) z;