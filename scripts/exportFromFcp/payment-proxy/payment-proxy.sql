select json_agg(z) from
    (SELECT p."applicationReference" as "reference",
            p."status",
            jsonb_strip_nulls(p.data) AS "data",
            jsonb_strip_nulls(p."paymentResponse") AS "paymentResponse",
            p."paymentCheckCount",
            p.frn::text,
             jsonb_strip_nulls(jsonb_build_object('legacyId', p.id)) AS "legacyData",
            jsonb_build_object('$date', TO_CHAR(p."createdAt" AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')) AS "createdAt",
            jsonb_build_object('$date', TO_CHAR(p."updatedAt" AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')) AS "updatedAt"
     FROM payment p
     ORDER BY p."createdAt") z;