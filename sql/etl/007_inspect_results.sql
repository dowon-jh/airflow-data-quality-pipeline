SELECT batch_id, processing_run_id, dag_run_id, ruleset_version, is_current, input_rows, clean_rows,
       quarantine_rows, warning_rows, issue_count, completed_at
FROM clean.processing_run ORDER BY batch_id, completed_at;
SELECT batch_id, COUNT(*) AS clean_rows FROM clean.ai_product_output GROUP BY batch_id;
SELECT batch_id, COUNT(*) AS quarantine_rows FROM quarantine.ai_product_output GROUP BY batch_id;
SELECT batch_id, rule_id, severity, COUNT(*) AS issues
FROM quarantine.dq_issue GROUP BY batch_id, rule_id, severity ORDER BY batch_id, rule_id;
SELECT COUNT(*) AS warning_issues_on_clean_rows
FROM clean.ai_product_output c JOIN quarantine.dq_issue i USING (batch_id, source_record_id)
WHERE i.severity = 'WARNING';
SELECT COUNT(*) AS clean_quarantine_overlap
FROM clean.ai_product_output c JOIN quarantine.ai_product_output q USING (batch_id, source_record_id);
SELECT COUNT(*) AS quarantine_business_values_differ_from_raw
FROM quarantine.ai_product_output q JOIN raw.ai_product_output r USING (batch_id, source_record_id)
WHERE ROW(q.product_id,q.category,q.original_text,q.ocr_text,q.extracted_attribute,q.attribute_type,q.model_version,q.created_at,q.ingested_at)
  IS DISTINCT FROM ROW(r.product_id,r.category,r.original_text,r.ocr_text,r.extracted_attribute,r.attribute_type,r.model_version,r.created_at,r.ingested_at);
SELECT COUNT(*) AS raw_rows FROM raw.ai_product_output;
