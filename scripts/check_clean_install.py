"""Reproduce the basic pipeline in a fresh, separate WSL Compose environment."""
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
DAG = "ai_output_quality_pipeline"
TABLES = ("raw.ingest_batch", "raw.ai_product_output", "clean.processing_run",
          "clean.ai_product_output", "quarantine.ai_product_output", "quarantine.dq_issue",
          "mart.daily_quality_metrics", "mart.daily_rule_metrics", "mart.batch_quality_gate")


def command(args, cwd=ROOT, env=None, input_text=None, timeout=60):
    result = subprocess.run(args, cwd=cwd, env=env, input=input_text, text=True,
                            capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError("Command failed: " + " ".join(args[:6]) + "\n" + result.stderr[-1600:])
    return result.stdout


def parse_json(output):
    for line in reversed(output.splitlines()):
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            pass
    raise ValueError("No JSON response")


def baseline():
    compose = ["docker", "compose", "-p", "ai-output-dq-pipeline"]
    hashes = {}
    for table in TABLES:
        sql = f"SELECT COALESCE(json_agg(x ORDER BY x::text),'[]'::json) FROM (SELECT row_to_json(t) x FROM {table} t) s;"
        output = command(compose + ["exec", "-T", "project-postgres", "psql", "-XAt",
                                    "-U", "etl_user", "-d", "etl_project", "-v", "ON_ERROR_STOP=1"],
                         input_text=sql)
        hashes[table] = {"rows": len(json.loads(output)),
                         "sha256": hashlib.sha256(output.encode()).hexdigest()}
    secret_dir = Path.home() / ".config/ai-output-dq-pipeline/secrets"
    files = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in
             (ROOT / ".env", secret_dir / "admin_password", secret_dir / "app_password")}
    containers = command(compose + ["ps", "--format", "{{.Service}} {{.ID}} {{.State}} {{.Health}}"])
    return {"tables": hashes, "config_secret_hashes": files, "containers": containers}


def main():
    if sys.platform != "linux" or "microsoft" not in os.uname().release.lower():
        raise RuntimeError("Run in Ubuntu WSL")
    if os.environ.get("PROJECT_COMPOSE_NAME") or os.environ.get("PROJECT_AIRFLOW_PORT"):
        raise RuntimeError("Run this verifier from the default environment without project overrides")
    with socket.socket() as port_check:
        port_check.bind(("127.0.0.1", 8081))
    suite = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid4().hex[:6]
    name = "ai-output-dq-clean-" + suite.lower()
    secret_dir = Path.home() / ".config" / name / "secrets"
    for suffix in ("project_pgdata", "airflow_home"):
        result = subprocess.run(["docker", "volume", "inspect", name + "_" + suffix],
                                text=True, capture_output=True)
        if result.returncode == 0 or "no such volume" not in result.stderr.lower():
            raise RuntimeError("Cannot confirm a new, absent volume")
    if secret_dir.exists():
        raise RuntimeError("Secret directory already exists")
    before = baseline()
    output_dir = ROOT / "data/generated/clean-install" / suite
    work = output_dir / "workspace"
    work.mkdir(parents=True)
    for folder in ("src", "sql", "scripts", "docs", "airflow/dags"):
        shutil.copytree(ROOT / folder, work / folder,
                        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    for file in ("docker-compose.yml", "README.md", "ROADMAP.md", "AIRFLOW_PROJECT_BRIEF.md", ".env.example"):
        shutil.copy2(ROOT / file, work / file)
    (work / "data").mkdir()
    env = os.environ.copy()
    env.update(PROJECT_COMPOSE_NAME=name, PROJECT_AIRFLOW_PORT="8081", PYTHONDONTWRITEBYTECODE="1")
    def compose(*args, **kwargs):
        return command(["docker", "compose", "-p", name, *args], cwd=work, env=env, **kwargs)
    def airflow(*args):
        return compose("exec", "-T", "airflow", "airflow", *args)
    def sql(query):
        return compose("exec", "-T", "project-postgres", "psql", "-XAt", "-U", "etl_user",
                       "-d", "etl_project", "-v", "ON_ERROR_STOP=1", input_text=query).strip()
    report = {"suite_id": suite, "project_name": name, "ui_port": 8081,
              "workspace": str(work), "secret_directory": str(secret_dir),
              "started_at": datetime.now(timezone.utc).isoformat(), "status": "RUNNING",
              "fresh_preflight": {"both_volumes_absent": True, "secrets_absent": True,
                                  "env_absent": not (work / ".env").exists(),
                                  "input_absent": not (work / "data/generated").exists()},
              "original_before": before}
    def save():
        (output_dir / "evidence.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    save()
    print("Fresh project:", name, "Evidence:", output_dir / "evidence.json", flush=True)
    try:
        setup = command(["python3", "scripts/setup_local.py"], cwd=work, env=env, timeout=180)
        (output_dir / "setup.log").write_text(setup)
        empty = {table: int(sql("SELECT COUNT(*) FROM " + table + ";")) for table in TABLES}
        assert all(count == 0 for count in empty.values()), empty
        report["empty_database_after_setup"] = empty
        print("Setup completed; all nine data tables are empty. Waiting for DAG registration.", flush=True)
        deadline = time.monotonic() + 600
        last_error = ""
        while time.monotonic() < deadline:
            try:
                dags = parse_json(airflow("dags", "list", "-o", "json"))
                if any(row["dag_id"] == DAG for row in dags):
                    break
            except (RuntimeError, ValueError) as exc:
                last_error = str(exc)
            time.sleep(5)
        else:
            raise RuntimeError("DAG registration timed out: " + last_error)
        errors = parse_json(airflow("dags", "list-import-errors", "-o", "json"))
        assert errors == [], errors
        report["import_errors"] = errors
        report["login_password_file_exists"] = compose("exec", "-T", "airflow", "test", "-s",
            "/opt/airflow/simple_auth_manager_passwords.json.generated") == ""
        command(["python3", "src/generator/generate_product_data.py"], cwd=work, env=env)
        manifest = json.loads((work / "data/generated/products.manifest.json").read_text())
        report["manifest"] = manifest
        trigger = command(["bash", "scripts/trigger_pipeline.sh"], cwd=work, env=env)
        (output_dir / "trigger.log").write_text(trigger)
        print("DAG registered; default 1,000-row input generated and triggered.", flush=True)
        deadline = time.monotonic() + 600
        while time.monotonic() < deadline:
            runs = parse_json(airflow("dags", "list-runs", DAG, "-o", "json"))
            if runs and runs[0]["state"] in ("success", "failed"):
                break
            time.sleep(5)
        else:
            raise RuntimeError("DAG completion timed out")
        assert len(runs) == 1 and runs[0]["state"] == "success", runs
        report["dag_runs"] = runs
        tasks = parse_json(airflow("tasks", "states-for-dag-run", DAG, runs[0]["run_id"], "-o", "json"))
        assert len(tasks) == 6 and all(t["state"] == "success" for t in tasks), tasks
        report["task_states"] = tasks
        rows = json.loads(sql("SELECT row_to_json(t) FROM (SELECT input_rows,clean_rows,quarantine_rows,"
                             "warning_rows,issue_count,ruleset_version FROM clean.processing_run WHERE is_current) t;"))
        assert rows == {"input_rows": 1000, "clean_rows": 959, "quarantine_rows": 41,
                        "warning_rows": 3, "issue_count": 44, "ruleset_version": "1.1"}, rows
        counts = {table: int(sql("SELECT COUNT(*) FROM " + table + ";")) for table in TABLES}
        assert counts["raw.ai_product_output"] == 1000 and counts["raw.ingest_batch"] == 1
        assert counts["clean.ai_product_output"] == 959 and counts["quarantine.ai_product_output"] == 41
        assert counts["quarantine.dq_issue"] == 44 and counts["clean.processing_run"] == 1
        assert counts["mart.daily_quality_metrics"] == 5 and counts["mart.batch_quality_gate"] == 1
        gate = json.loads(sql("SELECT row_to_json(t) FROM (SELECT decision,policy_version,"
                            "ROUND(actual_quarantine_rate*100,2) AS quarantine_percent FROM mart.batch_quality_gate) t;"))
        assert gate == {"decision": "PASS", "policy_version": "1.0", "quarantine_percent": 4.1}, gate
        permission = sql("SELECT rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls "
                         "FROM pg_roles WHERE rolname='etl_app';")
        assert permission == "f", permission
        report.update(results=rows, table_counts=counts, gate=gate, runtime_admin_flags_false=True)
        compose("stop", timeout=90)
        report["verification_services_stopped"] = True
        after = baseline()
        assert before == after, "Original environment changed"
        report.update(original_after=after, original_environment_unchanged=True, status="PASS",
                      completed_at=datetime.now(timezone.utc).isoformat())
        print("PASS: six tasks success, 1000/959/41/44, 5 mart groups, 4.1% PASS; original unchanged.", flush=True)
    except BaseException as exc:
        report.update(status="ERROR", error=type(exc).__name__ + ": " + str(exc))
        raise
    finally:
        save()
        print("Evidence:", output_dir / "evidence.json", flush=True)


if __name__ == "__main__":
    main()
