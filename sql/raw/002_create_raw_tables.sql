CREATE TABLE IF NOT EXISTS raw.ai_product_output (
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
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (batch_id, source_record_id)
);

CREATE TABLE IF NOT EXISTS raw.ingest_batch (
    batch_id TEXT PRIMARY KEY,
    data_sha256 TEXT NOT NULL CHECK (data_sha256 ~ '^[0-9a-f]{64}$'),
    row_count INTEGER NOT NULL CHECK (row_count > 0),
    source_file TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
