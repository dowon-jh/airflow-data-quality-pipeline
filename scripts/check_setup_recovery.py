"""Phase 8 setup recovery checks with real Compose/DB work and injected stage failures."""
from contextlib import redirect_stderr, redirect_stdout
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import sys
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]


def main():
    if sys.platform != "linux":
        raise SystemExit("Run in Ubuntu WSL")
    spec = importlib.util.spec_from_file_location("phase8_setup", ROOT / "scripts/setup_local.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    env_before = (ROOT / ".env").read_bytes()
    secret_dir = Path.home() / ".config/ai-output-dq-pipeline/secrets"
    secret_hashes = {name: hashlib.sha256((secret_dir/name).read_bytes()).hexdigest()
                     for name in ("admin_password", "app_password")}
    original_compose = module.compose
    reports = []
    staged_before = set(ROOT.glob(".env.setup.*"))
    output = io.StringIO()

    def fail_before_commit(env_file, *args, **kwargs):
        if kwargs.get("sql") is not None:
            raise RuntimeError("Phase 8 injected migration call failure before commit")
        return original_compose(env_file, *args, **kwargs)

    with redirect_stdout(output), redirect_stderr(output):
        with patch.object(module, "compose", fail_before_commit):
            try:
                module.main()
                raise AssertionError("Pre-commit setup failure did not fire")
            except RuntimeError as exc:
                assert "database migration" in str(exc)
        assert "Previous service state restored." in output.getvalue()
        assert (ROOT / ".env").read_bytes() == env_before
        assert set(ROOT.glob(".env.setup.*")) == staged_before
        reports.append({"scenario": "setup_precommit_failure", "env_unchanged": True,
                        "previous_service_state_restored": True, "temporary_config_removed": True})

        output.seek(0)
        output.truncate()
        with patch.object(module.os, "replace", side_effect=OSError("Phase 8 injected config publication failure")):
            try:
                module.main()
                raise AssertionError("Post-commit setup failure did not fire")
            except RuntimeError as exc:
                assert "config publication" in str(exc)
        assert "DB migration committed; keep current secret files" in output.getvalue()
        retained = set(ROOT.glob(".env.setup.*")) - staged_before
        assert len(retained) == 1 and (ROOT / ".env").read_bytes() == env_before
        # Recover forward with the existing secret files; admin SQL is idempotent.
        module.main()
        assert (ROOT / ".env").read_bytes() == env_before
        for path in retained:
            assert path.parent.resolve() == ROOT.resolve() and path.name.startswith(".env.setup.")
            path.unlink()  # Only this check's retained nonsecret staged config, after successful recovery.
        reports.append({"scenario": "setup_postcommit_failure", "retained_config": True,
                        "forward_recovery_succeeded": True, "env_unchanged": True})
    assert all(hashlib.sha256((secret_dir/name).read_bytes()).hexdigest() == value
               for name, value in secret_hashes.items())
    assert module.is_running("airflow") and module.is_running("project-postgres")
    result = {"status": "PASS", "secrets_unchanged": True, "services_running": True, "scenarios": reports}
    target = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "data/generated/phase8/setup-recovery.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
