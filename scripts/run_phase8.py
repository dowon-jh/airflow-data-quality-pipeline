"""Run isolated Phase 8 scenarios in WSL against the real local DAG and DB.

This stops ONLY project-postgres for DB outage scenarios and restores it in finally.
Inputs/evidence remain in data/generated/phase8/<suite-id>; no volume deletion.
"""
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
DAG = "ai_output_quality_pipeline"
TASKS = ("collect_input", "load_raw", "evaluate_quality", "load_results",
         "build_quality_metrics", "check_batch_thresholds")


def command(*args, input_text=None, timeout=30):
    result = subprocess.run(args, cwd=ROOT, input=input_text, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError("Command failed: " + " ".join(args[:5]) + "\n" + result.stderr[-1600:])
    return result.stdout


def compose(*args, **kwargs):
    return command("docker", "compose", *args, **kwargs)


def airflow(*args):
    return compose("exec", "-T", "airflow", "airflow", *args)


def parse_json(text):
    for line in reversed(text.splitlines()):
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    raise ValueError("No JSON output")


def probe(action, **kwargs):
    return parse_json(compose("exec", "-T", "airflow", "python", "-m", "validation.phase8_probe",
                              input_text=json.dumps({"action": action, **kwargs})))


def write_input(path, records, batch_id):
    data = "".join(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n" for row in records)
    path.write_text(data, encoding="utf-8")
    manifest = {"batch_id": batch_id, "rows": len(records), "data_sha256": hashlib.sha256(data.encode()).hexdigest(),
                "phase8_fixture": True}
    path.with_suffix(".manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


class Suite:
    def __init__(self):
        self.id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        self.root = ROOT / "data/generated/phase8" / self.id
        self.root.mkdir(parents=True, exist_ok=False)
        self.report = {"suite_id": self.id, "started_at": datetime.now(timezone.utc).isoformat(), "scenarios": []}
        self.dates = {}
        self.db_stopped = False
        self.base_date = datetime.now(timezone.utc).replace(microsecond=0)

    def save(self):
        (self.root / "evidence.json").write_text(json.dumps(self.report, ensure_ascii=False, indent=2) + "\n")

    def emit(self, scenario, **result):
        self.report["scenarios"].append({"scenario": scenario, **result})
        self.save()
        print(json.dumps({"scenario": scenario, **result}, ensure_ascii=False), flush=True)

    def fixture(self, name, records):
        records = [dict(row, batch_id=f"phase8-{self.id}-{name}") for row in records]
        path = self.root / (name + ".jsonl")
        write_input(path, records, f"phase8-{self.id}-{name}")
        return path, f"phase8-{self.id}-{name}"

    def start(self, name, path):
        run_id = f"phase8__{self.id}__{name}"
        logical_date = (self.base_date + timedelta(seconds=len(self.dates))).isoformat()
        self.dates[run_id] = logical_date
        conf = json.dumps({"input_file": path.relative_to(ROOT / "data").as_posix()})
        airflow("dags", "trigger", DAG, "-r", run_id, "-l", logical_date, "-c", conf, "-o", "json")
        return run_id

    def states(self, run_id):
        return {r["task_id"]: r["state"] for r in parse_json(airflow(
            "tasks", "states-for-dag-run", DAG, run_id, "-o", "json"))}

    def wait(self, run_id, state=None, task_id=None, timeout=260):
        deadline, previous = time.monotonic() + timeout, None
        while time.monotonic() < deadline:
            states = self.states(run_id)
            if states != previous:
                print(json.dumps({"run_id": run_id, "states": states}), flush=True)
                previous = states
            if task_id and states.get(task_id) == state:
                return states
            if not task_id and states and all(s in {"success", "failed", "upstream_failed", "skipped"} for s in states.values()):
                assert len(states) == len(TASKS)
                return states
            time.sleep(2)
        raise TimeoutError("Timed out waiting for " + run_id)

    def finish(self, name, run_id, expected_failure=None):
        states = self.wait(run_id)
        if expected_failure:
            assert states[expected_failure] == "failed", states
            position = TASKS.index(expected_failure)
            assert all(states[t] == "success" for t in TASKS[:position]), states
            assert all(states[t] == "upstream_failed" for t in TASKS[position+1:]), states
        else:
            assert all(states[t] == "success" for t in TASKS), states
        self.emit(name, run_id=run_id, states=states, expected_failure=expected_failure)
        self.report.setdefault("logs", {})[run_id] = probe("logs", run_id=run_id)
        self.save()
        return states

    def clear(self, run_id, task_regex, downstream=False):
        task_ids = [task for task in TASKS if re.fullmatch(task_regex, task)]
        if downstream:
            task_ids = list(TASKS[min(TASKS.index(task) for task in task_ids):])
        result = probe("clear", run_id=run_id, task_ids=task_ids)
        assert result["cleared"] and result["preview_count"] == len(task_ids)
        self.emit("targeted_clear", result=result)

    def stop_db(self):
        self.db_stopped = True
        compose("stop", "-t", "5", "project-postgres")

    def restore_db(self):
        compose("start", "project-postgres")
        for _ in range(20):
            response = subprocess.run(["docker", "compose", "exec", "-T", "project-postgres",
                                       "pg_isready", "-U", "etl_user", "-d", "etl_project"], cwd=ROOT,
                                      capture_output=True, timeout=10)
            if response.returncode == 0:
                self.db_stopped = False
                return
            time.sleep(1)
        raise RuntimeError("PostgreSQL was started but is not ready")

    def run(self):
        baseline_batches = ["batch-208ca532eb1f", "batch-a2ebfbe5e814"]
        baseline = {batch: probe("snapshot", batch_id=batch) for batch in baseline_batches}
        self.report["baseline"] = baseline
        self.save()
        base_path = self.root / "base.jsonl"
        command("python3", "src/generator/generate_product_data.py", "--rows", "100", "--error-rate", "0",
                "--null-rate", "0", "--output", str(base_path), "--batch-id", f"phase8-{self.id}-base")
        base = [json.loads(line) for line in base_path.read_text().splitlines()]
        airflow("dags", "unpause", DAG)

        normal_path, normal_batch = self.fixture("normal", base)
        normal_run = self.start("normal", normal_path)
        self.finish("normal_initial_load", normal_run)
        before = probe("snapshot", batch_id=normal_batch)
        assert before["raw.ai_product_output"]["rows"] == 100
        self.clear(normal_run, "^(load_results|build_quality_metrics|check_batch_thresholds)$")
        self.finish("same_run_task_clear", normal_run)
        assert len(probe("logs", run_id=normal_run)["load_results"]["attempts"]) == 2
        after = probe("snapshot", batch_id=normal_batch)
        for table in ("raw.ai_product_output", "clean.processing_run", "clean.ai_product_output", "mart.batch_quality_gate"):
            assert before[table] == after[table], table
        assert before["mart.daily_quality_metrics"]["rows"] == after["mart.daily_quality_metrics"]["rows"]
        self.emit("same_run_idempotency", processing_and_gate_history_unchanged=True, mart_rows_unchanged=True)

        row_fault = [dict(row) for row in base]
        row_fault[0]["extracted_attribute"] = None
        path, row_batch = self.fixture("row-error", row_fault)
        self.finish("single_row_quarantine", self.start("row-error", path))
        snap = probe("snapshot", batch_id=row_batch)
        assert snap["clean.ai_product_output"]["rows"] == 99 and snap["quarantine.ai_product_output"]["rows"] == 1

        for name, nulls in (("boundary5", 5), ("boundary6", 6), ("bad-model", 20)):
            records = [dict(row, model_version="v1.1" if name == "bad-model" else "v1.0") for row in base]
            for row in records[:nulls]:
                row["extracted_attribute"] = None
            path, batch = self.fixture(name, records)
            run_id = self.start(name, path)
            self.finish(name, run_id, "check_batch_thresholds" if nulls > 5 else None)
            snap = probe("snapshot", batch_id=batch)
            assert snap["quarantine.ai_product_output"]["rows"] == nulls
            assert sum(m["input_rows"] for m in snap["metrics"]) == 100
            assert snap["gates"][0]["decision"] == ("FAIL" if nulls > 5 else "PASS")
            self.emit(name + "_preserved", clean_rows=100-nulls, quarantine_rows=nulls,
                      raw_rows=100, mart_groups=len(snap["metrics"]), gate=snap["gates"][0]["decision"])
            if name == "bad-model":
                bad_before = snap
                self.clear(run_id, "^check_batch_thresholds$")
                self.finish("failed_gate_clear", run_id, "check_batch_thresholds")
                assert len(probe("logs", run_id=run_id)["check_batch_thresholds"]["attempts"]) == 2
                assert probe("snapshot", batch_id=batch) == bad_before
                corrected = [dict(row, model_version="v1.1") for row in base]
                corrected_path, corrected_batch = self.fixture("corrected", corrected)
                self.finish("corrected_new_batch", self.start("corrected", corrected_path))
                assert probe("snapshot", batch_id=batch)["raw.ai_product_output"] == bad_before["raw.ai_product_output"]
                self.emit("old_bad_raw_preserved", bad_batch=batch, corrected_batch=corrected_batch)

        dimension_rows = [dict(row) for row in base]
        for row, missing in zip(dimension_rows[:2], (None, "")):
            row.update(category=missing, model_version=missing, created_at=None)
        dimension_rows[2]["created_at"] = "2026-08-31T23:30:00-01:00"
        path, dimension_batch = self.fixture("dimensions", dimension_rows)
        self.finish("null_dimensions_and_date_fallback", self.start("dimensions", path))
        snap = probe("snapshot", batch_id=dimension_batch)
        assert sum(m["missing_created_at_rows"] for m in snap["metrics"]) == 2
        missing_groups = [m for m in snap["metrics"] if m["missing_created_at_rows"]]
        assert len(missing_groups) == 2 and {m["category"] for m in missing_groups} == {None, ""}
        assert {m["metric_date"] for m in missing_groups} == {datetime.now(timezone.utc).date().isoformat()}
        self.emit("dimension_evidence", metrics=snap["metrics"])

        missing = [dict(row) for row in base]
        missing[0].pop("category")
        path, missing_batch = self.fixture("missing-column", missing)
        self.finish("schema_failure", self.start("schema-failure", path), "collect_input")
        assert probe("snapshot", batch_id=missing_batch)["raw.ai_product_output"]["rows"] == 0
        path, empty_batch = self.fixture("empty", [])
        self.finish("empty_input_failure", self.start("empty", path), "collect_input")
        assert probe("snapshot", batch_id=empty_batch)["raw.ingest_batch"]["rows"] == 0

        conflict = [dict(row, batch_id=normal_batch) for row in base]
        conflict[0]["extracted_attribute"] = "999ml"
        path = self.root / "conflict.jsonl"
        write_input(path, conflict, normal_batch)
        before = probe("snapshot", batch_id=normal_batch)
        self.finish("same_batch_content_conflict", self.start("conflict", path), "load_raw")
        assert probe("snapshot", batch_id=normal_batch) == before

        # Real DB rollback after result deletes/inserts; no production failure switches.
        self.emit("result_rollback", result=probe("rollback_results", batch_id=row_batch))
        self.emit("mart_rollback", result=probe("rollback_mart", batch_id=row_batch))
        self.emit("concurrent_processing", result=probe("concurrent", batch_id=normal_batch))
        rerun = self.start("new-run-reprocess", normal_path)
        before = probe("snapshot", batch_id=normal_batch)
        self.finish("new_run_reprocessing", rerun)
        after = probe("snapshot", batch_id=normal_batch)
        assert after["raw.ai_product_output"] == before["raw.ai_product_output"]
        assert after["clean.ai_product_output"]["rows"] == 100 and after["current_runs"] == 1 and after["overlap"] == 0
        assert after["clean.processing_run"]["rows"] == before["clean.processing_run"]["rows"] + 1
        self.clear(normal_run, "^load_results$", downstream=True)
        self.finish("superseded_run_rejected", normal_run, "load_results")
        assert probe("snapshot", batch_id=normal_batch) == after

        # Transient outage: restore after observing Airflow's first retry wait.
        self.stop_db()
        try:
            run_id = self.start("db-transient", normal_path)
            self.wait(run_id, "up_for_retry", "load_raw", timeout=80)
            self.emit("db_transient_retry_observed", run_id=run_id, task="load_raw")
        finally:
            self.restore_db()
        self.finish("db_transient_recovered", run_id)
        retry_logs = probe("logs", run_id=run_id)
        assert len(retry_logs["load_raw"]["attempts"]) >= 2

        # Exhaust retries, then target this run only for recovery.
        self.stop_db()
        try:
            failed_run = self.start("db-exhausted", normal_path)
            self.finish("db_retries_exhausted", failed_run, "load_raw")
        finally:
            self.restore_db()
        outage_logs = probe("logs", run_id=failed_run)
        assert len(outage_logs["load_raw"]["attempts"]) == 3
        before = probe("snapshot", batch_id=normal_batch)
        self.clear(failed_run, "^load_raw$", downstream=True)
        self.finish("db_failed_run_clear_recovered", failed_run)
        after = probe("snapshot", batch_id=normal_batch)
        assert after["raw.ai_product_output"] == before["raw.ai_product_output"]
        assert after["clean.processing_run"]["rows"] == before["clean.processing_run"]["rows"] + 1
        assert after["current_runs"] == 1 and after["overlap"] == 0

        final_baseline = {batch: probe("snapshot", batch_id=batch) for batch in baseline_batches}
        for batch in baseline_batches:
            initial = {k:v for k,v in baseline[batch].items() if k != "raw_total"}
            final = {k:v for k,v in final_baseline[batch].items() if k != "raw_total"}
            assert initial == final, "Original batch changed: " + batch
        self.report["final_baseline"] = final_baseline
        self.report["completed_at"] = datetime.now(timezone.utc).isoformat()
        self.report["status"] = "PASS"
        self.save()
        self.emit("suite_complete", status="PASS", original_batches_unchanged=True,
                  final_raw_total=final_baseline[baseline_batches[0]]["raw_total"], evidence=str(self.root / "evidence.json"))


def main():
    if sys.platform != "linux":
        raise SystemExit("Run in Ubuntu WSL")
    suite = Suite()
    try:
        suite.run()
    except BaseException as exc:
        suite.report["status"] = "ERROR"
        suite.report["error"] = type(exc).__name__ + ": " + str(exc)
        suite.save()
        raise
    finally:
        if suite.db_stopped:
            suite.restore_db()


if __name__ == "__main__":
    main()
