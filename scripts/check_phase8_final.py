"""Read-only verification of the completed suite after setup recovery checks."""
import json
from pathlib import Path
import sys
from run_phase8 import airflow, compose, parse_json, probe


def main():
    evidence_path = Path(sys.argv[1])
    evidence = json.loads(evidence_path.read_text())
    assert evidence["status"] == "PASS"
    suite_id = evidence["suite_id"]
    for batch, previous in evidence["final_baseline"].items():
        assert probe("snapshot", batch_id=batch) == previous, "Original batch changed after setup recovery"
    runs = parse_json(airflow("dags", "list-runs", "ai_output_quality_pipeline", "-o", "json"))
    selected = [run for run in runs if run["run_id"].startswith(f"phase8__{suite_id}__")]
    failed_scenarios = {"normal", "boundary6", "bad-model", "schema-failure", "empty", "conflict"}
    assert len(selected) == 13
    for run in selected:
        name = run["run_id"].split("__")[-1]
        assert run["state"] == ("failed" if name in failed_scenarios else "success"), run
    services = compose("ps", "--format", "{{.Service}} {{.State}} {{.Health}}")
    assert "airflow running" in services and "project-postgres running healthy" in services
    result = {"status": "PASS", "original_batches_unchanged_after_setup": True,
              "dag_runs": len(selected), "successful_runs": 7, "expected_failed_runs": 6,
              "services_running": True, "raw_total": next(iter(evidence["final_baseline"].values()))["raw_total"]}
    evidence_path.with_name("final-check.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
