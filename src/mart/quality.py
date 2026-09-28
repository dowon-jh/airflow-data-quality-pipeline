"""Aggregate committed results and persist a batch gate before reporting failure."""
from collections import Counter, defaultdict
from datetime import timezone
from decimal import Decimal
import logging

from psycopg2.extras import RealDictCursor
from common.db import connect_project_db
from dq.engine import normalize, normalized_id
from dq.raw import read_raw_batch
from etl.process_batch import inspect_current

POLICY_VERSION = "1.0"
MAX_QUARANTINE_RATE = Decimal("0.05")
LOGGER = logging.getLogger(__name__)
COUNT_FIELDS = ("input_rows", "clean_rows", "quarantine_rows", "warning_rows", "null_attribute_rows",
                "duplicate_id_rows", "evidence_candidate_rows", "missing_created_at_rows", "issue_count")


def lock_current_run(cursor, outcome):
    batch_id, run_id = outcome["batch_id"], outcome["processing_run_id"]
    cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (batch_id,))
    cursor.execute("SELECT * FROM clean.processing_run WHERE processing_run_id=%s::uuid AND batch_id=%s",
                   (run_id, batch_id))
    run = cursor.fetchone()
    if run is None or not run["is_current"]:
        raise ValueError("Processing run was superseded or is missing; start a new DAG run")
    expected = outcome["summary"]
    for db_field, summary_field in (("input_rows", "input_rows"), ("clean_rows", "pass_rows"),
                                    ("quarantine_rows", "quarantine_rows"), ("warning_rows", "warning_rows"),
                                    ("issue_count", "issue_count"), ("issues_by_rule", "issues_by_rule")):
        if run[db_field] != expected[summary_field]:
            raise ValueError("Processing summary differs from committed results")
    if run["ruleset_version"] != outcome["ruleset_version"]:
        raise ValueError("Processing ruleset version mismatch")
    inspect_current(cursor, batch_id, expected, run_id)
    return run


def build_quality_metrics(outcome):
    connection = connect_project_db()
    try:
        with connection:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                run = lock_current_run(cursor, outcome)
                metadata, records = read_raw_batch(cursor, outcome["batch_id"])
                if metadata["data_sha256"] != run["source_data_sha256"]:
                    raise ValueError("RAW identity differs from processing run")
                cursor.execute("SELECT source_record_id FROM clean.ai_product_output WHERE batch_id=%s",
                               (outcome["batch_id"],))
                clean_ids = {row["source_record_id"] for row in cursor.fetchall()}
                cursor.execute("SELECT source_record_id, rule_id, severity FROM quarantine.dq_issue WHERE batch_id=%s",
                               (outcome["batch_id"],))
                issues = defaultdict(list)
                for issue in cursor.fetchall():
                    issues[issue["source_record_id"]].append(issue)
                groups = {}
                rule_totals = Counter()
                for row in records:
                    metric_date = (row["created_at"] or row["ingested_at"]).astimezone(timezone.utc).date()
                    key = (metric_date, normalized_id(row["category"]), normalize(row["model_version"]))
                    counts, rules = groups.setdefault(key, (Counter(), Counter()))
                    row_issues = issues[row["source_record_id"]]
                    row_rules = {issue["rule_id"] for issue in row_issues}
                    counts["input_rows"] += 1
                    counts["clean_rows" if row["source_record_id"] in clean_ids else "quarantine_rows"] += 1
                    counts["warning_rows"] += any(i["severity"] == "WARNING" for i in row_issues)
                    counts["null_attribute_rows"] += row["extracted_attribute"] is None
                    counts["duplicate_id_rows"] += "DQ003" in row_rules
                    counts["evidence_candidate_rows"] += "DQ010" in row_rules
                    counts["missing_created_at_rows"] += row["created_at"] is None
                    counts["issue_count"] += len(row_issues)
                    for issue in row_issues:
                        rules[(issue["rule_id"], issue["severity"])] += 1
                        rule_totals[issue["rule_id"]] += 1
                totals = Counter()
                for counts, _ in groups.values():
                    totals.update(counts)
                for field in ("input_rows", "clean_rows", "quarantine_rows", "warning_rows", "issue_count"):
                    if totals[field] != run[field]:
                        raise ValueError("Metric aggregation differs from processing summary")
                if dict(rule_totals) != run["issues_by_rule"]:
                    raise ValueError("Metric rule totals differ from processing summary")
                cursor.execute("DELETE FROM mart.daily_quality_metrics WHERE batch_id=%s", (outcome["batch_id"],))
                columns = ", ".join(COUNT_FIELDS)  # Fixed identifiers only.
                for (metric_date, category, model_version), (counts, rules) in groups.items():
                    cursor.execute(
                        "INSERT INTO mart.daily_quality_metrics (batch_id,processing_run_id,metric_date,category,"
                        "model_version," + columns + ") VALUES (" + ",".join(["%s"] * (5 + len(COUNT_FIELDS)))
                        + ") RETURNING metric_id",
                        (outcome["batch_id"], outcome["processing_run_id"], metric_date, category, model_version)
                        + tuple(counts[field] for field in COUNT_FIELDS),
                    )
                    metric_id = cursor.fetchone()["metric_id"]
                    for (rule_id, severity), count in rules.items():
                        cursor.execute("INSERT INTO mart.daily_rule_metrics VALUES (%s,%s,%s,%s)",
                                       (metric_id, rule_id, severity, count))
                result = {**outcome, "metric_groups": len(groups), "metric_totals": dict(totals)}
        LOGGER.info("Quality mart stored: batch=%s groups=%s totals=%s",
                    outcome["batch_id"], result["metric_groups"], result["metric_totals"])
        return result
    finally:
        connection.close()


def check_batch_thresholds(metrics):
    connection = connect_project_db()
    try:
        with connection:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                run = lock_current_run(cursor, metrics)
                cursor.execute("SELECT count(*) AS groups, sum(input_rows) AS input_rows, "
                               "sum(clean_rows) AS clean_rows, sum(quarantine_rows) AS quarantine_rows, "
                               "sum(warning_rows) AS warning_rows, sum(issue_count) AS issue_count, "
                               "bool_and(processing_run_id=%s::uuid) AS same_run "
                               "FROM mart.daily_quality_metrics WHERE batch_id=%s",
                               (metrics["processing_run_id"], metrics["batch_id"]))
                stored = cursor.fetchone()
                if stored["groups"] != metrics["metric_groups"] or not stored["same_run"]:
                    raise ValueError("Current mart is missing or belongs to a different processing run")
                for field in ("input_rows", "clean_rows", "quarantine_rows", "warning_rows", "issue_count"):
                    if stored[field] != run[field]:
                        raise ValueError("Current mart totals differ from processing summary")
                # Exact decimal threshold comparison, never rounded display percentages.
                decision = "PASS" if run["quarantine_rows"] <= run["input_rows"] * MAX_QUARANTINE_RATE else "FAIL"
                cursor.execute(
                    "INSERT INTO mart.batch_quality_gate (processing_run_id,policy_version,batch_id,dag_run_id,"
                    "input_rows,quarantine_rows,max_quarantine_rate,decision) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT (processing_run_id,policy_version) DO NOTHING",
                    (metrics["processing_run_id"], POLICY_VERSION, metrics["batch_id"], run["dag_run_id"],
                     run["input_rows"], run["quarantine_rows"], MAX_QUARANTINE_RATE, decision),
                )
                cursor.execute("SELECT * FROM mart.batch_quality_gate WHERE processing_run_id=%s::uuid "
                               "AND policy_version=%s", (metrics["processing_run_id"], POLICY_VERSION))
                gate = cursor.fetchone()
                if (gate["max_quarantine_rate"] != MAX_QUARANTINE_RATE or gate["decision"] != decision
                        or gate["input_rows"] != run["input_rows"] or gate["quarantine_rows"] != run["quarantine_rows"]
                        or gate["dag_run_id"] != run["dag_run_id"]):
                    raise ValueError("Existing gate decision conflicts with current policy or results")
        # This function returns after COMMIT, so the DAG can fail without losing its decision evidence.
        result = {"batch_id": metrics["batch_id"], "processing_run_id": metrics["processing_run_id"],
                  "policy_version": POLICY_VERSION, "decision": decision, "input_rows": gate["input_rows"],
                  "quarantine_rows": gate["quarantine_rows"],
                  "actual_quarantine_rate": str(gate["actual_quarantine_rate"]),
                  "max_quarantine_rate": str(MAX_QUARANTINE_RATE)}
        LOGGER.info("Batch quality gate: %s", result)
        return result
    finally:
        connection.close()
