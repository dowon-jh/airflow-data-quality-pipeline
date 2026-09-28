"""Atomically replace a batch's latest CLEAN/QUARANTINE results using etl_app."""
import argparse
from datetime import datetime, timezone
import json
import sys
from uuid import UUID, uuid4

import psycopg2
from psycopg2.extras import Json, RealDictCursor, execute_values
from common.db import connect_project_db
from dq.engine import RULESET_VERSION, evaluate_batch
from dq.raw import RAW_FIELDS, read_raw_batch, summarize_results

RESULT_FIELDS = RAW_FIELDS + (
    "quality_status", "evidence_status", "ruleset_version", "processing_run_id", "dag_run_id",
)


def inspect_current(cursor, batch_id, summary, run_id):
    cursor.execute(
        "SELECT (SELECT count(*) FROM clean.ai_product_output WHERE batch_id=%s) AS clean_rows, "
        "(SELECT count(*) FROM quarantine.ai_product_output WHERE batch_id=%s) AS quarantine_rows, "
        "(SELECT count(*) FROM quarantine.dq_issue WHERE batch_id=%s) AS issue_count, "
        "(SELECT count(DISTINCT source_record_id) FROM quarantine.dq_issue "
        " WHERE batch_id=%s AND severity='WARNING') AS warning_rows", (batch_id,) * 4,
    )
    counts = cursor.fetchone()
    expected = {"clean_rows": summary["pass_rows"], "quarantine_rows": summary["quarantine_rows"],
                "issue_count": summary["issue_count"], "warning_rows": summary["warning_rows"]}
    if dict(counts) != expected:
        raise ValueError("Stored result counts differ from evaluated batch")
    cursor.execute("SELECT rule_id, count(*) AS rows FROM quarantine.dq_issue "
                   "WHERE batch_id=%s GROUP BY rule_id", (batch_id,))
    if {r["rule_id"]: r["rows"] for r in cursor.fetchall()} != summary["issues_by_rule"]:
        raise ValueError("Stored rule counts differ from evaluated batch")
    cursor.execute(
        "SELECT EXISTS (SELECT 1 FROM clean.ai_product_output c JOIN quarantine.ai_product_output q "
        "USING (batch_id, source_record_id) WHERE c.batch_id=%s) AS overlapping, "
        "EXISTS (SELECT 1 FROM clean.ai_product_output c JOIN quarantine.dq_issue i "
        "USING (batch_id, source_record_id) WHERE c.batch_id=%s AND i.severity='ERROR') AS clean_error, "
        "EXISTS (SELECT 1 FROM quarantine.ai_product_output q WHERE q.batch_id=%s AND NOT EXISTS "
        "(SELECT 1 FROM quarantine.dq_issue i WHERE i.batch_id=q.batch_id "
        "AND i.source_record_id=q.source_record_id AND i.severity='ERROR')) AS missing_error, "
        "EXISTS (SELECT 1 FROM (SELECT processing_run_id FROM clean.ai_product_output WHERE batch_id=%s "
        "UNION ALL SELECT processing_run_id FROM quarantine.ai_product_output WHERE batch_id=%s "
        "UNION ALL SELECT processing_run_id FROM quarantine.dq_issue WHERE batch_id=%s) r "
        "WHERE r.processing_run_id <> %s::uuid) AS mixed_runs", (batch_id,) * 6 + (run_id,),
    )
    if any(cursor.fetchone().values()):
        raise ValueError("Stored result partition or processing-run identity is inconsistent")


def process_batch(batch_id, processing_run_id=None, dag_run_id=None, expected_quality=None):
    run_id = str(UUID(processing_run_id)) if processing_run_id else str(uuid4())
    if not batch_id.strip() or len(batch_id) > 200:
        raise ValueError("batch_id must contain 1-200 characters")
    if dag_run_id is not None and (not dag_run_id.strip() or len(dag_run_id) > 500):
        raise ValueError("dag_run_id must contain 1-500 characters")
    connection = connect_project_db()
    try:
        # READ COMMITTED sees a preceding loader's commit after waiting for its lock.
        with connection:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (batch_id,))
                metadata, raw_records = read_raw_batch(cursor, batch_id)
                results = evaluate_batch(raw_records)
                summary = summarize_results(results)
                if expected_quality is not None:
                    actual_quality = {"batch_id": batch_id, "ruleset_version": RULESET_VERSION,
                                      "source_data_sha256": metadata["data_sha256"], "summary": summary}
                    if actual_quality != expected_quality:
                        raise ValueError("Quality result changed between evaluation and result loading")
                cursor.execute("SELECT batch_id, ruleset_version, source_data_sha256, is_current, dag_run_id "
                               "FROM clean.processing_run WHERE processing_run_id=%s::uuid", (run_id,))
                previous = cursor.fetchone()
                if previous:
                    if (previous["batch_id"] != batch_id or previous["ruleset_version"] != RULESET_VERSION
                            or previous["source_data_sha256"] != metadata["data_sha256"]
                            or previous["dag_run_id"] != dag_run_id or not previous["is_current"]):
                        raise ValueError("Processing ID was used for a different or superseded run; use a new ID")
                    inspect_current(cursor, batch_id, summary, run_id)
                    return {"status": "already_processed", "batch_id": batch_id,
                            "processing_run_id": run_id, "ruleset_version": RULESET_VERSION, "summary": summary}
                processed_at = datetime.now(timezone.utc)
                cursor.execute("UPDATE clean.processing_run SET is_current=false WHERE batch_id=%s AND is_current",
                               (batch_id,))
                cursor.execute(
                    "INSERT INTO clean.processing_run (processing_run_id, batch_id, ruleset_version, dag_run_id, "
                    "source_data_sha256, input_rows, clean_rows, quarantine_rows, warning_rows, issue_count, "
                    "issues_by_rule, is_current, completed_at) "
                    "VALUES (%s::uuid,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,true,%s)",
                    (run_id, batch_id, RULESET_VERSION, dag_run_id, metadata["data_sha256"],
                     summary["input_rows"], summary["pass_rows"], summary["quarantine_rows"],
                     summary["warning_rows"], summary["issue_count"], Json(summary["issues_by_rule"]), processed_at),
                )
                # All deletes and inserts share the transaction: readers see old or new committed results.
                # Invalidate old aggregates in the same commit as replacing row results.
                cursor.execute("DELETE FROM mart.daily_quality_metrics WHERE batch_id=%s", (batch_id,))
                cursor.execute("DELETE FROM quarantine.dq_issue WHERE batch_id=%s", (batch_id,))
                cursor.execute("DELETE FROM quarantine.ai_product_output WHERE batch_id=%s", (batch_id,))
                cursor.execute("DELETE FROM clean.ai_product_output WHERE batch_id=%s", (batch_id,))
                clean_rows, quarantine_rows, issue_rows = [], [], []
                for raw, result in zip(raw_records, results):
                    values = dict(raw)
                    if result["decision"] == "PASS":
                        values.update(result["normalized"])
                    values.update(quality_status=result["decision"], evidence_status=result["evidence_status"],
                                  ruleset_version=RULESET_VERSION, processing_run_id=run_id, dag_run_id=dag_run_id)
                    destination = clean_rows if result["decision"] == "PASS" else quarantine_rows
                    destination.append(tuple(values[field] for field in RESULT_FIELDS) + (processed_at,))
                    for issue in result["issues"]:
                        issue_rows.append((batch_id, raw["source_record_id"], issue["rule_id"], issue["error_type"],
                                           issue["field_name"], issue["severity"], issue["error_message"],
                                           RULESET_VERSION, run_id, dag_run_id, processed_at))
                columns = ", ".join(RESULT_FIELDS)  # Constant identifiers, never derived from user input.
                if clean_rows:
                    execute_values(cursor, "INSERT INTO clean.ai_product_output (" + columns + ", processed_at) VALUES %s",
                                   clean_rows, page_size=1000)
                if quarantine_rows:
                    execute_values(cursor, "INSERT INTO quarantine.ai_product_output (" + columns + ", detected_at) VALUES %s",
                                   quarantine_rows, page_size=1000)
                if issue_rows:
                    execute_values(cursor, "INSERT INTO quarantine.dq_issue (batch_id, source_record_id, rule_id, "
                                   "error_type, field_name, severity, error_message, ruleset_version, processing_run_id, "
                                   "dag_run_id, detected_at) VALUES %s", issue_rows, page_size=1000)
                inspect_current(cursor, batch_id, summary, run_id)
                cursor.execute("UPDATE clean.processing_run SET completed_at=clock_timestamp() "
                               "WHERE processing_run_id=%s::uuid", (run_id,))
        return {"status": "processed", "batch_id": batch_id, "processing_run_id": run_id,
                "ruleset_version": RULESET_VERSION, "summary": summary}
    finally:
        connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--processing-run-id", help="Optional UUID for an idempotent retry of the current run")
    parser.add_argument("--dag-run-id", help="Optional Airflow run ID; NULL for manual processing")
    args = parser.parse_args()
    try:
        outcome = process_batch(args.batch_id, args.processing_run_id, args.dag_run_id)
    except (OSError, ValueError, KeyError, psycopg2.Error) as exc:
        print(f"Batch processing failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(outcome, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
