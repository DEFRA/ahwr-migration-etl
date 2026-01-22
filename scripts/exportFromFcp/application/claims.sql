select json_agg(z) from
    (select c.reference,
            c."applicationReference",
            jsonb_build_object('$date', TO_CHAR(c."createdAt" AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')) as "createdAt",
            jsonb_build_object('$date', TO_CHAR(c."updatedAt" AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')) as "updatedAt",
            c."createdBy",
            c."updatedBy",
            case when c.type = 'E' then 'FOLLOW_UP'
                 else 'REVIEW'
                end as "type",
            jsonb_strip_nulls(
                    c.data - ARRAY['herdId', 'herdAssociatedAt', 'herdVersion']
            || jsonb_build_object('dateOfVisit', CASE WHEN c.data -> 'dateOfVisit' IS NOT NULL THEN jsonb_build_object('$date', c.data -> 'dateOfVisit') END)
            || jsonb_build_object('dateOfTesting', CASE WHEN c.data -> 'dateOfTesting' IS NOT NULL THEN jsonb_build_object('$date', c.data -> 'dateOfTesting') END)
            )
                                                                                                                    AS data,
            upper(replace(s."status", ' ', '_')) as "status",
            '[]'::jsonb     as "statusHistory", --added later in ETL, needs extracting from azure table storage
             (SELECT COALESCE(
                             jsonb_strip_nulls(
                                     jsonb_build_object(
                                             'id', h."id",
                                             'version', h."version",
                                             'associatedAt', CASE WHEN c.data -> 'herdAssociatedAt' IS NOT NULL THEN jsonb_build_object('$date', c.data -> 'herdAssociatedAt') END,
                                             'name', h."herdName",
                                             'reasons', h."herdReasons",
                                             'cph', h."cph"
                                     )
                             ),
                             '{}'::jsonb
                     )
              FROM ( SELECT 1) dummy LEFT JOIN LATERAL -- this forces a row to always exist, and allows empty object for herd instead of null
                  (SELECT *
                   from public.herd
                   WHERE "id"::varchar = c.data ->> 'herdId'
             AND version::text = c.data ->> 'herdVersion'
             LIMIT 1) h ON true) as "herd",
       (SELECT COALESCE(
    jsonb_agg(
    jsonb_strip_nulls(
    jsonb_build_object(
    'id', uh."id",
    'note', uh."note",
    'createdBy', uh."createdBy",
    'createdAt', jsonb_build_object('$date', TO_CHAR(uh."createdAt" AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')),
    'updatedProperty', uh."updatedProperty",
    'newValue', uh."newValue",
    'oldValue', uh."oldValue",
    'eventType', uh."eventType"
    )
    ) ORDER BY uh."createdAt"
    ),
    '[]'::jsonb
    )
FROM public.claim_update_history uh
WHERE uh."reference" = c.reference) as "updateHistory"
from claim c
    left join public.status s on c."statusId" = s."statusId"
GROUP BY c.reference,
    c."applicationReference",
    c.type,
    c."createdAt",
    c."updatedAt",
    c."createdBy",
    c."updatedBy",
    c.data, s.status) z;