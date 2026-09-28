SELECT metric_date, batch_id, category, model_version, input_rows, clean_rows, quarantine_rows,
       warning_rows, null_attribute_rows, duplicate_id_rows, evidence_candidate_rows,
       missing_created_at_rows, issue_count, ROUND(quarantine_rate*100,2) AS quarantine_percent
FROM mart.current_daily_quality_metrics ORDER BY batch_id,metric_date,category,model_version;

SELECT m.batch_id, r.rule_id, r.severity, SUM(r.issue_count) AS issues
FROM mart.current_daily_quality_metrics m JOIN mart.daily_rule_metrics r USING (metric_id)
GROUP BY m.batch_id,r.rule_id,r.severity ORDER BY m.batch_id,r.rule_id;

SELECT g.batch_id,g.processing_run_id,g.dag_run_id,g.policy_version,g.input_rows,g.quarantine_rows,
       ROUND(g.actual_quarantine_rate*100,2) AS quarantine_percent,
       ROUND(g.max_quarantine_rate*100,2) AS threshold_percent,g.decision,g.checked_at,r.is_current
FROM mart.batch_quality_gate g JOIN clean.processing_run r USING (processing_run_id,batch_id)
ORDER BY g.checked_at;

SELECT COUNT(*) AS raw_rows FROM raw.ai_product_output;
