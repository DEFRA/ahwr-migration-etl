WITH doc_stats AS (
    SELECT
        jsonb_build_object(
            'total_rows', COUNT(*),
            'total_created', COUNT(*) FILTER (WHERE "status" = 'document-created'),
            'total_failed', COUNT(*) FILTER (WHERE "status" = 'send-failed'),
            'total_legacy_email_status', COUNT(*) FILTER (WHERE "status" like 'email%'),
            'first_created_doc', MIN("createdAt"),
            'last_created_doc', MAX("createdAt"),
            'avg_ts_epoch_created', AVG(EXTRACT(EPOCH FROM "createdAt")),
            'first_updated_doc', MIN("updatedAt"),
            'last_updated_doc', MAX("updatedAt"),
            'avg_ts_epoch_updated', AVG(EXTRACT(EPOCH FROM "updatedAt")),
            'lowest_sbi', min("sbi"),
            'highest_sbi', max("sbi"),
            'count_legacy_completed', COUNT(*) FILTER (WHERE "completed" IS NOT NULL),
            'all_legacy_id_md5', (SELECT
                              md5(
                                string_agg(
                                  concat_ws('|', id"),
                                  '|' ORDER BY id
                                )
                              ) FROM document_log),
            'all_legacy_ref_md5', (SELECT
                              md5(
                                string_agg(
                                  concat_ws('|', "emailReference"),
                                  '|' ORDER BY "emailReference"
                                )
                              ) FROM document_log where "emailReference" IS NOT NULL)
        ) AS doc_metrics
    FROM document_log
)
SELECT * FROM doc_stats;