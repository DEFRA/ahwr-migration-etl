WITH msg_stats AS (
    SELECT
        jsonb_build_object(
            'total_rows', COUNT(*),
            'total_new_claim', COUNT(*) FILTER (WHERE "messageType" = 'claimCreated'),
            'total_evidence_email', COUNT(*) FILTER (WHERE "messageType" = 'statusChange-5'),
            'first_created_doc', MIN("createdAt"),
            'last_created_doc', MAX("createdAt"),
            'avg_ts_epoch_created', AVG(EXTRACT(EPOCH FROM "createdAt")),
            'first_updated_doc', MIN("updatedAt"),
            'last_updated_doc', MAX("updatedAt"),
            'avg_ts_epoch_updated', AVG(EXTRACT(EPOCH FROM "updatedAt")),
            'lowest_claim_ref', MIN("claimReference"),
            'highest_claim_ref', MAX("claimReference"),
            'lowest_app_ref', MIN("agreementReference"),
            'highest_app_ref', MAX("agreementReference"),
            'all_legacy_field_md5', (SELECT
                              md5(
                                string_agg(
                                  concat_ws('|', id),
                                  '|' ORDER BY id
                                )
                              )
                            FROM message_generate)
        ) AS msg_metrics
    FROM message_generate
)
SELECT * FROM msg_stats;