-- claims per application
SELECT
    json_agg(
        json_build_object(
            'reference', "applicationReference",
            'claims_count', claims_count
        )
        ORDER BY claims_count DESC
    ) AS results
FROM (
    SELECT
        "applicationReference",
        COUNT(*) AS claims_count
    FROM claim
    GROUP BY "applicationReference"
) t;


--overall herds count
SELECT COUNT(*) AS herd_flock_count
FROM herd;

--herd per application
SELECT
    json_agg(
        json_build_object(
            'reference', "applicationReference",
            'herd_count', herd_count
        )
        ORDER BY herd_count DESC
    ) AS results
FROM (
    SELECT
        COUNT(*) AS herd_count,
        h."applicationReference"
    FROM herd h
    GROUP BY h."applicationReference"
) t;


-- herds per species
SELECT
    json_agg(
        json_build_object(
            'species', species,
            'herd_count', herd_count
        )
        ORDER BY herd_count DESC
    ) AS results
FROM (
    SELECT
        COUNT(*) AS herd_count,
        h."species" AS species
    FROM herd h
    GROUP BY h."species"
) t;

--claims per herd
SELECT jsonb_agg(
    jsonb_build_object(
        'claims_count', cc.claims_count,
        'id', cc.id,
        'version', cc.version
    )
) AS herds
FROM (
    SELECT
        COUNT(*) AS claims_count,
        c.data->>'herdId' AS id,
        (c.data->>'herdVersion')::int AS version
    FROM claim c
    WHERE c.data ? 'herdId'
    GROUP BY c.data->>'herdId', (c.data->>'herdVersion')::int
    ORDER BY claims_count DESC, id DESC
) cc;