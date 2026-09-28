-- Latest row-level results plus successful processing-run summaries.
CREATE TABLE IF NOT EXISTS clean.processing_run (
    processing_run_id UUID PRIMARY KEY,
    batch_id TEXT NOT NULL REFERENCES raw.ingest_batch(batch_id),
    ruleset_version TEXT NOT NULL,
    dag_run_id TEXT,
    source_data_sha256 TEXT NOT NULL CHECK (source_data_sha256 ~ '^[0-9a-f]{64}$'),
    input_rows INTEGER NOT NULL CHECK (input_rows > 0),
    clean_rows INTEGER NOT NULL CHECK (clean_rows >= 0),
    quarantine_rows INTEGER NOT NULL CHECK (quarantine_rows >= 0),
    warning_rows INTEGER NOT NULL CHECK (warning_rows >= 0 AND warning_rows <= input_rows),
    issue_count INTEGER NOT NULL CHECK (issue_count >= quarantine_rows),
    issues_by_rule JSONB NOT NULL CHECK (jsonb_typeof(issues_by_rule) = 'object'),
    is_current BOOLEAN NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (clean_rows + quarantine_rows = input_rows),
    UNIQUE (processing_run_id, batch_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS processing_run_one_current_batch
    ON clean.processing_run(batch_id) WHERE is_current;

CREATE TABLE IF NOT EXISTS clean.ai_product_output (
    batch_id TEXT NOT NULL,
    source_record_id BIGINT NOT NULL,
    product_id TEXT NOT NULL,
    category TEXT NOT NULL,
    original_text TEXT,
    ocr_text TEXT,
    extracted_attribute TEXT NOT NULL,
    attribute_type TEXT,
    model_version TEXT,
    created_at TIMESTAMPTZ,
    ingested_at TIMESTAMPTZ NOT NULL,
    quality_status TEXT NOT NULL CHECK (quality_status = 'PASS'),
    evidence_status TEXT NOT NULL CHECK (evidence_status IN ('CHECKED','UNPARSEABLE','NO_SUPPORTED_QUANTITY')),
    ruleset_version TEXT NOT NULL,
    processing_run_id UUID NOT NULL,
    dag_run_id TEXT,
    processed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (batch_id, source_record_id),
    UNIQUE (batch_id, product_id),
    FOREIGN KEY (batch_id, source_record_id) REFERENCES raw.ai_product_output(batch_id, source_record_id),
    FOREIGN KEY (processing_run_id, batch_id) REFERENCES clean.processing_run(processing_run_id, batch_id)
);

CREATE TABLE IF NOT EXISTS quarantine.ai_product_output (
    batch_id TEXT NOT NULL,
    source_record_id BIGINT NOT NULL,
    product_id TEXT,
    category TEXT,
    original_text TEXT,
    ocr_text TEXT,
    extracted_attribute TEXT,
    attribute_type TEXT,
    model_version TEXT,
    created_at TIMESTAMPTZ,
    ingested_at TIMESTAMPTZ NOT NULL,
    quality_status TEXT NOT NULL CHECK (quality_status = 'QUARANTINE'),
    evidence_status TEXT NOT NULL CHECK (evidence_status IN ('CHECKED','UNPARSEABLE','NO_SUPPORTED_QUANTITY')),
    ruleset_version TEXT NOT NULL,
    processing_run_id UUID NOT NULL,
    dag_run_id TEXT,
    detected_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (batch_id, source_record_id),
    FOREIGN KEY (batch_id, source_record_id) REFERENCES raw.ai_product_output(batch_id, source_record_id),
    FOREIGN KEY (processing_run_id, batch_id) REFERENCES clean.processing_run(processing_run_id, batch_id)
);

CREATE TABLE IF NOT EXISTS quarantine.dq_issue (
    batch_id TEXT NOT NULL,
    source_record_id BIGINT NOT NULL,
    rule_id TEXT NOT NULL CHECK (rule_id ~ '^DQ[0-9]{3}$'),
    error_type TEXT NOT NULL,
    field_name TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('ERROR','WARNING')),
    error_message TEXT NOT NULL,
    ruleset_version TEXT NOT NULL,
    processing_run_id UUID NOT NULL,
    dag_run_id TEXT,
    detected_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (batch_id, source_record_id, rule_id),
    FOREIGN KEY (batch_id, source_record_id) REFERENCES raw.ai_product_output(batch_id, source_record_id),
    FOREIGN KEY (processing_run_id, batch_id) REFERENCES clean.processing_run(processing_run_id, batch_id)
);
