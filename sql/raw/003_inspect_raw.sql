SELECT schema_name
FROM information_schema.schemata
WHERE schema_name IN ('raw', 'clean', 'quarantine', 'mart')
ORDER BY schema_name;

SELECT batch_id, row_count, data_sha256, loaded_at
FROM raw.ingest_batch
ORDER BY loaded_at;

SELECT
    batch_id,
    COUNT(*) AS raw_rows,
    COUNT(*) FILTER (WHERE product_id IS NULL) AS null_product_ids,
    COUNT(*) FILTER (WHERE extracted_attribute IS NULL) AS null_attributes,
    COUNT(*) FILTER (WHERE extracted_attribute IS NOT NULL
        AND BTRIM(extracted_attribute) = '') AS empty_attributes
FROM raw.ai_product_output
GROUP BY batch_id
ORDER BY batch_id;

SELECT batch_id, COUNT(*) AS duplicate_product_id_groups
FROM (
    SELECT batch_id, product_id
    FROM raw.ai_product_output
    WHERE product_id IS NOT NULL
    GROUP BY batch_id, product_id
    HAVING COUNT(*) > 1
) duplicates
GROUP BY batch_id
ORDER BY batch_id;
