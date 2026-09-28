"""Shared RAW reader and quality-summary definitions."""
from collections import Counter

RAW_FIELDS = (
    "batch_id", "source_record_id", "product_id", "category", "original_text", "ocr_text",
    "extracted_attribute", "attribute_type", "model_version", "created_at", "ingested_at",
)


def read_raw_batch(cursor, batch_id):
    cursor.execute("SELECT row_count, data_sha256 FROM raw.ingest_batch WHERE batch_id=%s", (batch_id,))
    metadata = cursor.fetchone()
    if metadata is None:
        raise ValueError("Batch does not exist in raw.ingest_batch")
    cursor.execute(
        "SELECT batch_id, source_record_id, product_id, category, original_text, ocr_text, "
        "extracted_attribute, attribute_type, model_version, created_at, ingested_at "
        "FROM raw.ai_product_output WHERE batch_id=%s ORDER BY source_record_id", (batch_id,)
    )
    records = cursor.fetchall()
    if len(records) != metadata["row_count"]:
        raise ValueError("RAW count differs from committed ingest metadata")
    if not records:
        raise ValueError("Cannot process an empty RAW batch")
    return metadata, records


def summarize_results(results):
    rules = Counter(issue["rule_id"] for row in results for issue in row["issues"])
    quarantined = sum(row["decision"] == "QUARANTINE" for row in results)
    warnings = sum(any(i["severity"] == "WARNING" for i in row["issues"]) for row in results)
    return {"input_rows": len(results), "pass_rows": len(results) - quarantined,
            "quarantine_rows": quarantined, "warning_rows": warnings,
            "issue_count": sum(rules.values()), "issues_by_rule": dict(sorted(rules.items())),
            "quarantine_rate": quarantined / len(results)}
