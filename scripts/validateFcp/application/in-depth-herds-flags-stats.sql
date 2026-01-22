with herd_missings as (
SELECT
  COUNT(*) FILTER (WHERE "applicationReference" IS NULL) AS missing_applicationReference,
  COUNT(*) FILTER (WHERE "herdName" IS NULL OR "herdName" = '') AS missing_herdName,
  COUNT(*) FILTER (WHERE "cph" IS NULL OR "cph" = '') AS missing_cph,
  COUNT(*) FILTER (WHERE "species" IS NULL OR "species" = '') AS missing_species
FROM herd),
-- To find out orphaned herds - The herd applicationReference should point to an application.reference for this

orphaned_herds as (SELECT count(*) as orphaned_count
FROM herd h
LEFT JOIN application a
  ON h."applicationReference" = a."reference"
WHERE a."reference" IS NULL)

SELECT jsonb_build_object(
  'missing_applicationReference', hm.missing_applicationReference,
  'missing_herdName', hm.missing_herdName,
  'missing_cph', hm.missing_cph,
  'missing_species', hm.missing_species,
  'orphaned_count', oh.orphaned_count
) AS result
FROM herd_missings hm, orphaned_herds oh;



WITH flag_stats AS (
    SELECT
        jsonb_build_object(
            'total_rows', COUNT(*),
            'total_active', COUNT(*) FILTER (WHERE "deletedBy" IS NULL),
            'total_deleted', COUNT(*) FILTER (WHERE "deletedBy" IS NOT NULL),
            'first_created_flag', MIN("createdAt"),
            'last_created_flag', MAX("createdAt"),
            'avg_ts_epoch_created', AVG(EXTRACT(EPOCH FROM "createdAt")),
            'first_deleted_flag', MIN("deletedAt"),
            'last_deleted_flag', MAX("deletedAt"),
            'avg_ts_epoch_deleted', AVG(EXTRACT(EPOCH FROM "deletedAt")),
            'all_notes_md5', (SELECT
                              md5(
                                    string_agg(
                                            concat_ws('|', note, "deletedNote"),
                                            '|' ORDER BY id
                                    )
                                 )
                            FROM flag)
        ) AS flag_metrics
    FROM flag
)
SELECT * FROM flag_stats;
