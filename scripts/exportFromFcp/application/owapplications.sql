select json_agg(z) from
    (WITH date_of_claim_lookup AS (
        SELECT *
        FROM (
                 VALUES
                     ('AHWR-20FE-537B', '2023-03-01T15:34:36.506Z'::timestamptz),
                     ('AHWR-717C-0F2F', '2023-01-17T16:18:54.364Z'::timestamptz),
                     ('AHWR-891D-9B33', '2023-02-08T17:22:34.524Z'::timestamptz),
                     ('AHWR-0A4C-DCDD', '2022-11-22T13:19:44.973Z'::timestamptz),
                     ('AHWR-DEDC-2F8E', '2023-02-27T11:17:31.490Z'::timestamptz),
                     ('AHWR-54CE-1982', '2023-02-23T16:33:14.976Z'::timestamptz),
                     ('AHWR-DA25-2E04', '2023-02-14T09:47:51.541Z'::timestamptz),
                     ('AHWR-CDB9-31A7', '2023-02-28T11:01:47.690Z'::timestamptz),
                     ('AHWR-0C4F-8C79', '2024-01-22T16:43:20.798Z'::timestamptz),
                     ('AHWR-7926-6018', '2022-11-11T14:24:31.116Z'::timestamptz),
                     ('AHWR-0205-EB5C', '2023-01-18T06:26:49.968Z'::timestamptz),
                     ('AHWR-BETA-0004', '2023-11-22T07:10:36.905Z'::timestamptz),
                     ('AHWR-4097-B0B0', '2023-02-10T17:52:34.492Z'::timestamptz),
                     ('AHWR-5C90-E8E8', '2023-01-19T19:26:38.759Z'::timestamptz),
                     ('AHWR-1BCD-9FD4', '2023-03-15T10:21:34.885Z'::timestamptz),
                     ('AHWR-D676-4159', '2023-02-24T13:14:53.081Z'::timestamptz),
                     ('AHWR-E141-238A', '2023-12-10T16:24:12.452Z'::timestamptz),
                     ('AHWR-A244-9411', '2023-03-08T11:39:44.581Z'::timestamptz),
                     ('AHWR-7BAF-BB94', '2023-01-25T19:17:09.303Z'::timestamptz),
                     ('AHWR-0665-5424', '2023-03-02T05:16:24.493Z'::timestamptz)
             ) AS v(reference, date_of_claim)
    )
     select a.reference,
            jsonb_build_object('$date', TO_CHAR(a."createdAt" AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')) as "createdAt",
            jsonb_build_object('$date', TO_CHAR(a."updatedAt" AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')) as "updatedAt",
            a."createdBy",
            a."updatedBy",
            jsonb_strip_nulls(
                    jsonb_build_object(
                            'confirmCheckDetails', a.data -> 'confirmCheckDetails',
                            'offerStatus', a.data -> 'offerStatus',
                            'declaration', a.data -> 'declaration',
                            'reference', a.data -> 'reference',
                            'whichReview', a.data -> 'whichReview',
                            'eligibleSpecies', a.data -> 'eligibleSpecies',
                            'vetName', a.data -> 'vetName',
                            'vetRcvs', a.data -> 'vetRcvs',
                            'urnResult', a.data -> 'urnResult',
                            'detailsCorrect', a.data -> 'detailsCorrect',
                            'visitDate', CASE WHEN a.data -> 'visitDate' IS NOT NULL THEN jsonb_build_object('$date', a.data -> 'visitDate') END,
                            'dateOfClaim', CASE
                                               WHEN COALESCE(
                                                       a.data -> 'dateOfClaim',
                                                       to_jsonb(date_of_claim_lookup.date_of_claim)
                                                    ) IS NOT NULL
                                                   THEN jsonb_build_object(
                                                       '$date',
                                                       COALESCE(
                                                               a.data ->> 'dateOfClaim',
           TO_CHAR(
             date_of_claim_lookup.date_of_claim AT TIME ZONE 'UTC',
             'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
           )
         )
                                                        )
                                END,
                            'animalsTested', a.data -> 'animalsTested',
                            'dateOfTesting', a.data -> 'dateOfTesting'
                    ))                                             AS data,
            a.data -> 'organisation'                               AS organisation,
            upper(replace(s."status", ' ', '_')) as "status",
            '[]'::jsonb                                            as "statusHistory", --added in downstream ETL, from azure table storage
             (SELECT COALESCE(
                             jsonb_agg(
                                     jsonb_strip_nulls(
                                             jsonb_build_object(
                                                     'id', uh."id",
                                                     'note', uh."note",
                                                     'createdBy', uh."createdBy",
                                                     'createdAt', jsonb_build_object('$date', uh."createdAt"),
                                                     'updatedProperty', uh."updatedProperty",
                                                     'newValue', uh."newValue",
                                                     'oldValue', uh."oldValue",
                                                     'eventType', uh."eventType"
                                             )
                                     ) ORDER BY uh."createdAt"
                             ),
                             '[]'::jsonb
                     )
              FROM (SELECT id,
                           note,
                           "createdBy",
                           TO_CHAR("createdAt" AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') as "createdAt",
                           "updatedProperty",
                           "newValue",
                           "oldValue",
                           "eventType"
                    FROM public.application_update_history
                    WHERE "applicationReference" = a.reference
                    UNION ALL
                    SELECT id,
                           note,
                           "createdBy",
                           TO_CHAR("createdAt" AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') as "createdAt",
                           "updatedProperty",
                           "newValue",
                           "oldValue",
                           "eventType"
                    FROM public.claim_update_history
                    WHERE "applicationReference" = a.reference) uh) as "updateHistory",
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
             WHERE ch."applicationReference" = a.reference)        AS "contactHistory",
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
WHERE f."applicationReference" = a.reference)         AS flags,
    claimed,
    "eligiblePiiRedaction"
from application a
    left join public.status s on a."statusId" = s."statusId"
    left join date_of_claim_lookup on a.reference = date_of_claim_lookup.reference
where a.type = 'VV'
GROUP BY a.reference,
    a."createdAt",
    a."updatedAt",
    a."createdBy",
    a."updatedBy",
    a.data, s.status, claimed, "eligiblePiiRedaction", date_of_claim_lookup.date_of_claim) z;