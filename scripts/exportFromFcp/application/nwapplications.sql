select json_agg(z) from
    (select a.reference,
            jsonb_build_object('$date', TO_CHAR(a."createdAt" AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')) as "createdAt",
            jsonb_build_object('$date', TO_CHAR(a."updatedAt" AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')) as "updatedAt",
            a."createdBy",
            a."updatedBy",
            jsonb_strip_nulls(
                    jsonb_build_object(
                            'confirmCheckDetails', a.data -> 'confirmCheckDetails',
                            'offerStatus', a.data -> 'offerStatus',
                            'declaration', a.data -> 'declaration',
                            'reference', a.data -> 'reference'
                    )
            ) AS data,
            a.data -> 'organisation'                        AS organisation,
            upper(replace(s."status", ' ', '_')) as "status",
            '[]'::jsonb                                     as "statusHistory", --added in downstream ETL, from azure table storage
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
              FROM public.application_update_history uh
              WHERE uh."applicationReference" = a.reference) as "updateHistory", -- for new world just this. For old world we need to check the claim update history table
            (SELECT COALESCE(
                            jsonb_agg(
                                    jsonb_strip_nulls(
                                            jsonb_build_object(
                                                    'id', ch."id",
                                                    'field', ch.data -> 'field',
                                                    'oldValue', ch.data -> 'oldValue',
                                                    'newValue', ch.data -> 'newValue',
                                                    'createdAt', jsonb_build_object('$date', TO_CHAR(ch."createdAt" AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'))
                                            )
                                    ) ORDER BY ch."createdAt"
                            ),
                            '[]'::jsonb
                    )
             FROM public.contact_history ch
             WHERE ch."applicationReference" = a.reference) AS "contactHistory",
            (SELECT COALESCE(
                            jsonb_strip_nulls(
                                    jsonb_build_object(
                                            'id', ar."id",
                                            'retryCount', ar."retryCount",
                                            'createdBy', ar."createdBy",
                                            'createdAt', CASE WHEN ar."createdAt" IS NOT NULL THEN jsonb_build_object('$date', TO_CHAR(ar."createdAt" AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')) END,
                                            'requestedDate', CASE WHEN ar."requestedDate" IS NOT NULL THEN jsonb_build_object('$date', TO_CHAR(ar."requestedDate" AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')) END,
                                            'status', ar."status",
                                            'data', ar."data",
                                            'success', ar."success",
                                            'redactedSbi', ar."redactedSbi"
                                    )
                            ),
                            '{}'::jsonb
                    )
             FROM ( SELECT 1) dummy LEFT JOIN LATERAL -- this forces a row to always exist, and allows empty object for herd instead of null
                 (SELECT *
                  from public.application_redact
                  WHERE reference = a.reference
                     LIMIT 1) ar ON true)  AS "redactionHistory",
       (SELECT COALESCE(
    jsonb_agg(
    jsonb_strip_nulls(
    jsonb_build_object(
    'id', f."id",
    'note', f."note",
    'createdBy', f."createdBy",
    'createdAt', jsonb_build_object('$date', TO_CHAR(f."createdAt" AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')),
    'deletedBy', f."deletedBy",
    'deletedAt', CASE WHEN f."deletedAt" IS NOT NULL THEN jsonb_build_object('$date', TO_CHAR(f."deletedAt" AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')) END,
    'deletedNote', f."deletedNote",
    'appliesToMh', f."appliesToMh",
    'deleted', (f."deletedBy" IS NOT NULL)
    )
    ) ORDER BY f.id
    ),
    '[]'::jsonb
    )
FROM public.flag f
WHERE f."applicationReference" = a.reference)  AS flags,
    "eligiblePiiRedaction"
from application a
    left join public.status s on a."statusId" = s."statusId"
where a.type = 'EE'
GROUP BY a.reference,
    a."createdAt",
    a."updatedAt",
    a."createdBy",
    a."updatedBy",
    a.data, s.status, "eligiblePiiRedaction") z;