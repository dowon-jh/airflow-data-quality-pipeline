"""Read-only Phase 4 review: no role, data, service or secret changes."""
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from dq.engine import evaluate_batch, quantities


def command(args, input=None):
    result = subprocess.run(args, cwd=ROOT, input=input, text=True, capture_output=True)
    if result.returncode:
        raise RuntimeError("Read-only audit command failed; captured output is withheld")
    return result.stdout


for service in ("airflow", "project-postgres"):
    container = command(["docker", "compose", "ps", "-q", service]).strip()
    config = json.loads(command(["docker", "inspect", container]))[0]
    keys = [item.split("=", 1)[0] for item in config["Config"]["Env"]]
    print(json.dumps({"service": service,
                      "password_value_env_keys": [key for key in keys if "PASSWORD" in key and not key.endswith("_FILE")],
                      "legacy_connection_uri": "AIRFLOW_CONN_PROJECT_POSTGRES" in keys,
                      "privileged": config["HostConfig"]["Privileged"],
                      "port_bindings": config["HostConfig"]["PortBindings"],
                      "secret_mounts": [{"destination": mount["Destination"], "writable": mount["RW"]}
                                        for mount in config["Mounts"] if mount["Destination"].startswith("/run/secrets/")],
                      "docker_socket_mounted": any(m["Destination"] == "/var/run/docker.sock" for m in config["Mounts"])},
                     ensure_ascii=False))

runtime = '''
import json
from common.db import connect_project_db
c = connect_project_db()
try:
    c.set_session(readonly=True)
    with c.cursor() as q:
        q.execute("SELECT current_user, rolsuper, rolcreatedb, rolcreaterole FROM pg_roles WHERE rolname=current_user")
        print("runtime_role:", q.fetchone())
        q.execute("SELECT role.rolname FROM pg_auth_members membership JOIN pg_roles member ON member.oid=membership.member JOIN pg_roles role ON role.oid=membership.roleid WHERE member.rolname=current_user")
        print("role_memberships:", q.fetchall())
        q.execute("SELECT datname, has_database_privilege(current_user, oid, 'CONNECT'), has_database_privilege(current_user, oid, 'TEMP') FROM pg_database WHERE datallowconn ORDER BY datname")
        print("database_connect_temp:", q.fetchall())
        q.execute("SELECT batch_id, count(*) FROM raw.ai_product_output GROUP BY batch_id ORDER BY batch_id")
        print("runtime_raw_counts:", q.fetchall())
finally:
    c.close()
'''
print(command(["docker", "compose", "exec", "-T", "airflow", "python", "-"], runtime).strip())

base = {"batch_id": "review-only", "source_record_id": 1, "product_id": "P000001",
        "category": "TUMBLER", "original_text": "용량 500ml", "ocr_text": "용량 500ml",
        "extracted_attribute": "500ml"}
for name, overrides in (
    ("attribute_200", {"extracted_attribute": "가" * 200}),
    ("attribute_201", {"extracted_attribute": "가" * 201}),
    ("null_attribute", {"extracted_attribute": None}),
    ("empty_attribute", {"extracted_attribute": "  "}),
    ("ocr_warning", {"ocr_text": "용량 800ml"}),
    ("quantity_comma", {"extracted_attribute": "1,500ml"}),
    ("quantity_comma_source", {"original_text": "용량 1,500ml", "ocr_text": "용량 1,500ml", "extracted_attribute": "500ml"}),
    ("quantity_signed_suffix", {"original_text": "용량 -500ml", "ocr_text": "용량 -500ml", "extracted_attribute": "x-500ml"}),
):
    row = base | overrides
    result = evaluate_batch([row])[0]
    print(json.dumps({"case": name, "decision": result["decision"],
                      "evidence_status": result["evidence_status"],
                      "rules": [issue["rule_id"] for issue in result["issues"]],
                      "attribute_tokens": sorted(str(number) + unit for number, unit in quantities(row["extracted_attribute"]))},
                     ensure_ascii=False))
duplicates = evaluate_batch([base, base | {"source_record_id": 2, "product_id": " p000001 "}])
print("normalized_duplicate_rules:", [[i["rule_id"] for i in row["issues"]] for row in duplicates])

report_path = ROOT / "data/generated/quality-report.json"
report = json.loads(report_path.read_text())
print("saved_report:", json.dumps({"batch_id": report["batch_id"], "ruleset_version": report["ruleset_version"], "summary": report["summary"]}))
