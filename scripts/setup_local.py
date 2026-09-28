"""Prepare the local WSL environment with staged config and failure recovery."""
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
SECRET_VALUES = []
PROJECT_NAME = os.environ.get("PROJECT_COMPOSE_NAME", "ai-output-dq-pipeline")
AIRFLOW_PORT = os.environ.get("PROJECT_AIRFLOW_PORT", "8080")


def redact(message):
    for value in SECRET_VALUES:
        message = message.replace(value, "[REDACTED]")
    message = re.sub(r"(?i)(PASSWORD\s+)'[^']*'", r"\1'[REDACTED]'", message)
    return message[-2000:]


def run(arguments, *, sql=None):
    result = subprocess.run(arguments, cwd=ROOT, input=sql, text=True,
                            capture_output=True, check=False)
    if result.returncode:
        detail = redact(result.stderr or result.stdout)
        raise RuntimeError("Command failed: " + " ".join(arguments[:4]) + "\n" + detail)
    return result.stdout


def compose(env_file, *arguments, sql=None):
    return run(["docker", "compose", "-p", PROJECT_NAME, "--env-file", str(env_file), *arguments], sql=sql)


def is_running(service):
    output = run(["docker", "ps", "--filter",
                  "label=com.docker.compose.project=" + PROJECT_NAME, "--filter",
                  "label=com.docker.compose.service=" + service, "--format", "{{.ID}}"])
    return bool(output.strip())


def main():
    if sys.platform != "linux" or "microsoft" not in os.uname().release.lower():
        raise RuntimeError("Run this script in Ubuntu WSL.")
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,62}", PROJECT_NAME):
        raise RuntimeError("Invalid PROJECT_COMPOSE_NAME")
    if not AIRFLOW_PORT.isdecimal() or not 1024 <= int(AIRFLOW_PORT) <= 65535:
        raise RuntimeError("PROJECT_AIRFLOW_PORT must be between 1024 and 65535")
    env_path = ROOT / ".env"
    if env_path.is_symlink():
        raise RuntimeError("Refusing a symlink at .env")
    old_bytes = env_path.read_bytes() if env_path.exists() else None
    old = dict(line.split("=", 1) for line in (old_bytes.decode().splitlines() if old_bytes else [])
               if line and not line.startswith("#") and "=" in line)
    for key, expected in (("PROJECT_COMPOSE_NAME", PROJECT_NAME), ("PROJECT_AIRFLOW_PORT", AIRFLOW_PORT)):
        if old.get(key, expected) != expected:
            raise RuntimeError("Configured " + key + " differs; use the original environment settings.")
    for key, expected in (("PROJECT_DB_USER", "etl_user"), ("PROJECT_DB_NAME", "etl_project")):
        if old.get(key, expected) != expected:
            raise RuntimeError("This setup expects " + key + "=" + expected)
    # No files or services change before Docker and existing-volume preflight.
    run(["docker", "info", "--format", "{{.ServerVersion}}"])
    volume = subprocess.run(["docker", "volume", "inspect", PROJECT_NAME + "_project_pgdata"],
                            text=True, capture_output=True)
    if volume.returncode and "no such volume" not in volume.stderr.lower():
        raise RuntimeError("Cannot determine existing DB volume state; no secrets were generated.")
    existing_database = volume.returncode == 0
    secret_dir = Path.home() / ".config" / PROJECT_NAME / "secrets"
    if old.get("PROJECT_SECRET_DIR", str(secret_dir)) != str(secret_dir):
        raise RuntimeError("Configured secret location differs from this WSL home; restore the original files first.")
    if secret_dir.is_symlink():
        raise RuntimeError("Refusing a symlink at the secret directory")
    for name in ("admin_password", "app_password"):
        target = secret_dir / name
        if target.is_symlink():
            raise RuntimeError("Refusing a symlink at a secret path")
        if existing_database and not target.is_file():
            raise RuntimeError("Existing DB secret is missing; restore it. No new password was generated.")
    sql = "BEGIN;\n" + "\n".join((ROOT / file).read_text() for file in (
        "sql/schemas/001_create_layers.sql", "sql/raw/002_create_raw_tables.sql",
        "sql/etl/006_create_result_tables.sql",
        "sql/mart/008_create_quality_mart.sql",
        "sql/security/004_runtime_role.sql")) + "\nCOMMIT;\n"
    before = {service: is_running(service) for service in ("project-postgres", "airflow")}
    secret_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    secret_dir.chmod(0o700)
    for name in ("admin_password", "app_password"):
        target = secret_dir / name
        if not target.exists():
            descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "w") as handle:
                handle.write(secrets.token_hex(24) + "\n")
        password = target.read_text().strip()
        if not re.fullmatch(r"[0-9a-f]{48}", password):
            raise RuntimeError("Secret must contain the original 48 hex characters; restore the file.")
        SECRET_VALUES.append(password)
        target.chmod(0o444)
    config = ("PROJECT_DB_USER=etl_user\nPROJECT_DB_NAME=etl_project\n"
              + f"PROJECT_SECRET_DIR={secret_dir}\nPROJECT_COMPOSE_NAME={PROJECT_NAME}\n"
              + f"PROJECT_AIRFLOW_PORT={AIRFLOW_PORT}\n")
    descriptor, name = tempfile.mkstemp(prefix=".env.setup.", dir=ROOT)
    staged = Path(name)
    with os.fdopen(descriptor, "w") as handle:
        handle.write(config)
    database_committed = published = False
    stage = "compose validation"
    try:
        compose(staged, "config", "--quiet")
        print("Configuration validated. Airflow remains running during DB preparation.", flush=True)
        stage = "postgres startup"
        compose(staged, "up", "-d", "--wait", "--wait-timeout", "30", "project-postgres")
        stage = "database migration"
        compose(staged, "exec", "-T", "project-postgres", "psql", "-X", "-U", "etl_user",
                "-d", "etl_project", "-v", "ON_ERROR_STOP=1", sql=sql)
        database_committed = True
        stage = "config publication"
        os.replace(staged, env_path)
        published = True
        stage = "airflow startup"
        compose(env_path, "up", "-d", "airflow")
    except (OSError, RuntimeError) as exc:
        if not database_committed:
            print("Migration not committed; previous .env retained.", file=sys.stderr)
            if old_bytes is not None:
                try:
                    if before["project-postgres"]:
                        compose(env_path, "up", "-d", "--wait", "--wait-timeout", "30", "project-postgres")
                    if before["airflow"]:
                        compose(env_path, "up", "-d", "airflow")
                    if not before["project-postgres"]:
                        compose(env_path, "stop", "project-postgres")
                    print("Previous service state restored.", file=sys.stderr)
                except (OSError, RuntimeError) as recovery:
                    print("Automatic recovery failed:", redact(str(recovery)), file=sys.stderr)
                    print("After resolving the cause, rerun python3 scripts/setup_local.py.", file=sys.stderr)
        else:
            # Credentials/DDL may already be committed. Recover forward, never use stale config.
            print("DB migration committed; keep current secret files and rerun python3 scripts/setup_local.py.",
                  file=sys.stderr)
            if published:
                print("Airflow resume command: docker compose up -d airflow", file=sys.stderr)
            else:
                print("Prepared nonsecret config retained:", staged, file=sys.stderr)
        raise RuntimeError("Failed during " + stage + ": " + redact(str(exc))) from None
    finally:
        if staged.exists() and not database_committed:
            staged.unlink()
    print("Setup complete. Runtime role: etl_app. Existing volumes retained.")
    print("Secret directory (values not displayed):", secret_dir)


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, UnicodeError) as exc:
        print("Setup failed:", redact(str(exc)), file=sys.stderr)
        sys.exit(1)
