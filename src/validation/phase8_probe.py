"""Phase 8 integration probes. Never imported by the production DAG."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import sys
from threading import Barrier
from unittest.mock import patch
from uuid import uuid4

from psycopg2.extras import RealDictCursor
from common.db import connect_project_db
from etl.process_batch import process_batch
from mart.quality import build_quality_metrics, check_batch_thresholds

TABLES = ("raw.ingest_batch", "raw.ai_product_output", "clean.processing_run", "clean.ai_product_output",
          "quarantine.ai_product_output", "quarantine.dq_issue", "mart.daily_quality_metrics",
          "mart.batch_quality_gate")


def snapshot(batch_id):
    connection = connect_project_db()
    try:
        connection.set_session(isolation_level="REPEATABLE READ", readonly=True)
        with connection:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                result = {}
                for table in TABLES:
                    cursor.execute("SELECT * FROM " + table + " WHERE batch_id=%s", (batch_id,))
                    rows = [dict(row) for row in cursor.fetchall()]
                    serialized = sorted(json.dumps(row, sort_keys=True, default=str) for row in rows)
                    result[table] = {"rows": len(rows), "sha256": hashlib.sha256(
                        json.dumps(serialized).encode()).hexdigest()}
                    if table == "clean.processing_run":
                        result["current_runs"] = sum(r["is_current"] for r in rows)
                    if table == "mart.batch_quality_gate":
                        result["gates"] = [{k: r[k] for k in ("processing_run_id", "policy_version", "decision",
                                                             "input_rows", "quarantine_rows")} for r in rows]
                cursor.execute("SELECT r.* FROM mart.daily_rule_metrics r JOIN mart.daily_quality_metrics m "
                               "USING(metric_id) WHERE m.batch_id=%s", (batch_id,))
                rows = sorted(json.dumps(dict(row), sort_keys=True) for row in cursor.fetchall())
                result["mart.daily_rule_metrics"] = {"rows": len(rows), "sha256": hashlib.sha256(
                    json.dumps(rows).encode()).hexdigest()}
                cursor.execute("SELECT metric_date,category,model_version,input_rows,clean_rows,quarantine_rows,"
                               "null_attribute_rows,missing_created_at_rows FROM mart.current_daily_quality_metrics "
                               "WHERE batch_id=%s ORDER BY metric_id", (batch_id,))
                result["metrics"] = [dict(row) for row in cursor.fetchall()]
                cursor.execute("SELECT count(*) AS rows FROM raw.ai_product_output")
                result["raw_total"] = cursor.fetchone()["rows"]
                cursor.execute("SELECT count(*) AS rows FROM clean.ai_product_output c "
                               "JOIN quarantine.ai_product_output q USING(batch_id,source_record_id) "
                               "WHERE c.batch_id=%s", (batch_id,))
                result["overlap"] = cursor.fetchone()["rows"]
                return result
    finally:
        connection.close()


def rollback_results(batch_id):
    import etl.process_batch as module
    before = snapshot(batch_id)
    run_id = str(uuid4())
    original = module.inspect_current
    injected = []

    def fail_after_writes(*args):
        original(*args)
        injected.append(True)
        raise RuntimeError("Phase 8 injected failure after result writes, before commit")

    with patch.object(module, "inspect_current", fail_after_writes):
        try:
            module.process_batch(batch_id, processing_run_id=run_id)
            raise AssertionError("Result fault did not fire")
        except RuntimeError as exc:
            assert "Phase 8 injected" in str(exc)
    after = snapshot(batch_id)
    assert injected and before == after, "Result rollback changed persisted data"
    return {"scenario": "result_rollback", "injected_after_writes": True,
            "full_snapshot_unchanged": True, "attempted_run_id": run_id}


class CursorProxy:
    def __init__(self, cursor, fired):
        self.cursor, self.fired = cursor, fired

    def __enter__(self):
        self.cursor.__enter__()
        return self

    def __exit__(self, *args):
        return self.cursor.__exit__(*args)

    def __getattr__(self, name):
        return getattr(self.cursor, name)

    def execute(self, sql, *args):
        result = self.cursor.execute(sql, *args)
        if sql.startswith("INSERT INTO mart.daily_rule_metrics"):
            self.fired.append(True)
            raise RuntimeError("Phase 8 injected failure after mart writes, before commit")
        return result


class ConnectionProxy:
    def __init__(self, connection, fired):
        self.connection, self.fired = connection, fired

    def __enter__(self):
        self.connection.__enter__()
        return self

    def __exit__(self, *args):
        return self.connection.__exit__(*args)

    def cursor(self, *args, **kwargs):
        return CursorProxy(self.connection.cursor(*args, **kwargs), self.fired)

    def close(self):
        self.connection.close()


def rollback_mart(batch_id):
    import mart.quality as module
    connection = connect_project_db()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT processing_run_id,dag_run_id FROM clean.processing_run "
                           "WHERE batch_id=%s AND is_current", (batch_id,))
            run = cursor.fetchone()
    finally:
        connection.close()
    outcome = process_batch(batch_id, str(run["processing_run_id"]), run["dag_run_id"])
    before, fired = snapshot(batch_id), []
    with patch.object(module, "connect_project_db", lambda: ConnectionProxy(connect_project_db(), fired)):
        try:
            module.build_quality_metrics(outcome)
            raise AssertionError("Mart fault did not fire")
        except RuntimeError as exc:
            assert "Phase 8 injected" in str(exc)
    assert fired and snapshot(batch_id) == before, "Mart rollback changed persisted data"
    return {"scenario": "mart_rollback", "injected_after_writes": True, "full_snapshot_unchanged": True}


def concurrent_processing(batch_id):
    import etl.process_batch as module
    before = snapshot(batch_id)
    run_id = str(uuid4())
    ready = Barrier(2)

    def synchronized_connection():
        connection = connect_project_db()
        ready.wait(timeout=10)
        return connection

    with patch.object(module, "connect_project_db", synchronized_connection):
        with ThreadPoolExecutor(max_workers=2) as executor:
            outcomes = list(executor.map(lambda _: module.process_batch(batch_id, run_id), range(2)))
    assert sorted(o["status"] for o in outcomes) == ["already_processed", "processed"]
    after = snapshot(batch_id)
    assert after["clean.processing_run"]["rows"] == before["clean.processing_run"]["rows"] + 1
    assert after["current_runs"] == 1 and after["overlap"] == 0
    assert after["raw.ai_product_output"] == before["raw.ai_product_output"]
    assert after["mart.daily_quality_metrics"]["rows"] == 0, "Previous mart was not invalidated"
    metrics = build_quality_metrics(outcomes[0])
    gate = check_batch_thresholds(metrics)
    assert gate["decision"] == "PASS"
    return {"scenario": "concurrent_same_uuid", "statuses": [o["status"] for o in outcomes],
            "new_processing_rows": 1, "current_runs": 1, "mart_invalidated_then_rebuilt": True,
            "processing_run_id": run_id}


def logs(run_id):
    root = Path("/opt/airflow/logs/dag_id=ai_output_quality_pipeline") / ("run_id=" + run_id)
    result = {}
    for task_dir in root.glob("task_id=*"):
        attempts, events = [], []
        for path in sorted(task_dir.glob("attempt=*.log")):
            attempts.append(path.stem)
            for line in path.read_text().splitlines():
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                event = str(entry.get("event", ""))
                if event.startswith(("Batch quality gate:", "Results stored:", "Quality mart stored:",
                                     "Loaded batch ", "Batch phase8")) or "Task failed with exception" in event:
                    events.append({"timestamp": entry.get("timestamp"), "event": event,
                                   "attempt": path.stem})
        result[task_dir.name.removeprefix("task_id=")] = {"attempts": attempts, "events": events}
    return result


def clear_tasks(run_id, task_ids):
    import requests
    if not run_id.startswith("phase8__"):
        raise ValueError("Clear is restricted to Phase 8 test runs")
    password = json.loads(Path("/opt/airflow/simple_auth_manager_passwords.json.generated").read_text())["admin"]
    token_response = requests.post("http://127.0.0.1:8080/auth/token",
                                   json={"username": "admin", "password": password}, timeout=10)
    token_response.raise_for_status()
    headers = {"Authorization": "Bearer " + token_response.json()["access_token"]}
    url = "http://127.0.0.1:8080/api/v2/dags/ai_output_quality_pipeline/clearTaskInstances"
    body = {"dag_run_id": run_id, "task_ids": task_ids, "only_failed": False, "only_running": False,
            "reset_dag_runs": True, "include_upstream": False, "include_downstream": False,
            "include_past": False, "include_future": False, "dry_run": True}
    preview = requests.post(url, json=body, headers=headers, timeout=10)
    preview.raise_for_status()
    selected = preview.json()["task_instances"]
    assert selected and {row["dag_run_id"] for row in selected} == {run_id}
    assert {row["task_id"] for row in selected} == set(task_ids)
    body["dry_run"] = False
    applied = requests.post(url, json=body, headers=headers, timeout=10)
    applied.raise_for_status()
    return {"run_id": run_id, "task_ids": task_ids, "preview_count": len(selected), "cleared": True}


def main():
    request = json.load(sys.stdin)
    action = request["action"]
    if action == "snapshot":
        result = snapshot(request["batch_id"])
    elif action == "rollback_results":
        result = rollback_results(request["batch_id"])
    elif action == "rollback_mart":
        result = rollback_mart(request["batch_id"])
    elif action == "concurrent":
        result = concurrent_processing(request["batch_id"])
    elif action == "logs":
        result = logs(request["run_id"])
    elif action == "clear":
        result = clear_tasks(request["run_id"], request["task_ids"])
    else:
        raise ValueError("Unknown probe action")
    print(json.dumps(result, default=str))


if __name__ == "__main__":
    main()
