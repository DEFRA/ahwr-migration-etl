select json_agg(z) from
    (select jsonb_strip_nulls(
                    jsonb_build_object(
                            'id', "id",
                            'version', "version",
                            'applicationReference', "applicationReference",
                            'name', "herdName",
                            'cph', "cph",
                            'reasons', "herdReasons",
                            'isCurrent', "isCurrent",
                            'species', species,
                            'createdAt', jsonb_build_object('$date', TO_CHAR("createdAt" AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')),
                            'updatedAt', jsonb_build_object('$date', TO_CHAR("updatedAt" AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')),
                            'createdBy', "createdBy",
                            'updatedBy', "updatedBy"
                    )
            ) as jason
     from herd) z;