-- Latest batch metrics, rule detail, and immutable-by-application gate decisions.
CREATE TABLE IF NOT EXISTS mart.daily_quality_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    batch_id TEXT NOT NULL,
    processing_run_id UUID NOT NULL,
    metric_date DATE NOT NULL,
    category TEXT,
    model_version TEXT,
    input_rows INTEGER NOT NULL CHECK (input_rows > 0),
    clean_rows INTEGER NOT NULL CHECK (clean_rows >= 0),
    quarantine_rows INTEGER NOT NULL CHECK (quarantine_rows >= 0),
    warning_rows INTEGER NOT NULL CHECK (warning_rows BETWEEN 0 AND input_rows),
    null_attribute_rows INTEGER NOT NULL CHECK (null_attribute_rows BETWEEN 0 AND input_rows),
    duplicate_id_rows INTEGER NOT NULL CHECK (duplicate_id_rows BETWEEN 0 AND input_rows),
    evidence_candidate_rows INTEGER NOT NULL CHECK (evidence_candidate_rows BETWEEN 0 AND input_rows),
    missing_created_at_rows INTEGER NOT NULL CHECK (missing_created_at_rows BETWEEN 0 AND input_rows),
    issue_count INTEGER NOT NULL CHECK (issue_count >= quarantine_rows),
    pass_rate NUMERIC GENERATED ALWAYS AS (clean_rows::numeric / input_rows) STORED,
    quarantine_rate NUMERIC GENERATED ALWAYS AS (quarantine_rows::numeric / input_rows) STORED,
    null_attribute_rate NUMERIC GENERATED ALWAYS AS (null_attribute_rows::numeric / input_rows) STORED,
    aggregated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (clean_rows + quarantine_rows = input_rows),
    UNIQUE NULLS NOT DISTINCT (batch_id, metric_date, category, model_version),
    FOREIGN KEY (processing_run_id, batch_id) REFERENCES clean.processing_run(processing_run_id, batch_id)
);

CREATE TABLE IF NOT EXISTS mart.daily_rule_metrics (
    metric_id BIGINT NOT NULL REFERENCES mart.daily_quality_metrics(metric_id) ON DELETE CASCADE,
    rule_id TEXT NOT NULL CHECK (rule_id ~ '^DQ[0-9]{3}$'),
    severity TEXT NOT NULL CHECK (severity IN ('ERROR','WARNING')),
    issue_count INTEGER NOT NULL CHECK (issue_count > 0),
    PRIMARY KEY (metric_id, rule_id, severity)
);

CREATE TABLE IF NOT EXISTS mart.batch_quality_gate (
    processing_run_id UUID NOT NULL,
    policy_version TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    dag_run_id TEXT,
    input_rows INTEGER NOT NULL CHECK (input_rows > 0),
    quarantine_rows INTEGER NOT NULL CHECK (quarantine_rows BETWEEN 0 AND input_rows),
    max_quarantine_rate NUMERIC NOT NULL CHECK (max_quarantine_rate BETWEEN 0 AND 1),
    actual_quarantine_rate NUMERIC GENERATED ALWAYS AS (quarantine_rows::numeric / input_rows) STORED,
    decision TEXT NOT NULL CHECK (decision IN ('PASS','FAIL')),
    checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK ((decision='PASS') = (quarantine_rows <= input_rows * max_quarantine_rate)),
    PRIMARY KEY (processing_run_id, policy_version),
    FOREIGN KEY (processing_run_id, batch_id) REFERENCES clean.processing_run(processing_run_id, batch_id)
);

CREATE OR REPLACE VIEW mart.current_daily_quality_metrics AS
SELECT m.* FROM mart.daily_quality_metrics m
JOIN clean.processing_run r USING (processing_run_id, batch_id) WHERE r.is_current;
