WITH pay_stats AS (
    SELECT
        jsonb_build_object(
            'total_rows', COUNT(*),
            'first_created_doc', MIN("createdAt"),
            'last_created_doc', MAX("createdAt"),
            'avg_ts_epoch_created', AVG(EXTRACT(EPOCH FROM "createdAt")),
            'first_updated_doc', MIN("updatedAt"),
            'last_updated_doc', MAX("updatedAt"),
            'avg_ts_epoch_updated', AVG(EXTRACT(EPOCH FROM "updatedAt")),
            'lowest_ref', MIN("applicationReference"),
            'highest_ref', MAX("applicationReference"),
            'lowest_frn', MIN("frn"),
            'highest_frn', MAX("frn"),
            'payment_check_totalsum', SUM("paymentCheckCount"),
            'all_legacy_field_md5', (SELECT
                              md5(
                                string_agg(
                                  concat_ws('|', id),
                                  '|' ORDER BY id
                                )
                              ) AS msg_combined_hash
                            FROM payment),
           'counts_by_status', (
                SELECT jsonb_object_agg("status", status_count)
                FROM (
                    SELECT
                        "status",
                        COUNT(*) AS status_count
                    FROM payment
                    GROUP BY "status"
                ) t)
        ) AS pay_metrics
    FROM payment
)
SELECT * FROM pay_stats;