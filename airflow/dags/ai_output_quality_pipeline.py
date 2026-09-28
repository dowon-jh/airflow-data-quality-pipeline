"""Collect, ingest, evaluate, split-load, aggregate, and apply the batch quality gate."""
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG, Param, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException


def fail_on_invalid_input(function, *args):
    # Deterministic contract/input errors need correction; infrastructure errors may retry.
    try:
        return function(*args)
    except (ValueError, KeyError, FileNotFoundError, UnicodeError) as exc:
        raise AirflowFailException(str(exc)) from exc


with DAG(
    dag_id="ai_output_quality_pipeline",
    description="Load product quality results, aggregate metrics, and apply the batch gate",
    start_date=datetime(2026, 9, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    dagrun_timeout=timedelta(minutes=20),
    is_paused_upon_creation=True,
    default_args={"owner": "dq-project", "retries": 2, "retry_delay": timedelta(seconds=30),
                  "retry_exponential_backoff": True, "max_retry_delay": timedelta(minutes=2),
                  "execution_timeout": timedelta(minutes=5)},
    params={"input_file": Param("generated/products.jsonl", type="string", minLength=1,
                                maxLength=300, description="Project data directory relative input JSONL path")},
    tags=["project", "dq", "phase7"],
    doc_md="""## RAW → quality → split load
Manual runs only. `input_file` is relative to `/opt/airflow/project/data`.
Prepare an input JSONL and its manifest before triggering. Label files are excluded.
Record errors are quarantined; contract/infrastructure failures block downstream tasks.
The final gate fails when batch quarantine rate exceeds 5%, after persisting results and metrics.
""",
) as dag:

    @task(multiple_outputs=False)
    def collect_input():
        from etl.pipeline import collect_input as collect
        return fail_on_invalid_input(collect, get_current_context()["params"]["input_file"])

    @task(multiple_outputs=False)
    def load_raw(source):
        from etl.pipeline import ingest_input
        return fail_on_invalid_input(ingest_input, source)

    @task(multiple_outputs=False)
    def evaluate_quality(source):
        from etl.pipeline import evaluate_input
        return fail_on_invalid_input(evaluate_input, source)

    @task(multiple_outputs=False)
    def load_results(quality):
        from etl.pipeline import persist_results
        context = get_current_context()
        return fail_on_invalid_input(persist_results, quality, context["dag"].dag_id, context["run_id"])

    @task(multiple_outputs=False)
    def build_quality_metrics(outcome):
        from mart.quality import build_quality_metrics as build
        return fail_on_invalid_input(build, outcome)

    @task(multiple_outputs=False)
    def check_batch_thresholds(metrics):
        from mart.quality import check_batch_thresholds as check
        gate = fail_on_invalid_input(check, metrics)
        if gate["decision"] == "FAIL":
            raise AirflowFailException(
                f"Batch {gate['batch_id']} quality gate failed: quarantine rate "
                f"{gate['actual_quarantine_rate']} > {gate['max_quarantine_rate']} "
                f"(policy {gate['policy_version']}); results and metrics were preserved"
            )
        return gate

    check_batch_thresholds(build_quality_metrics(load_results(evaluate_quality(load_raw(collect_input())))))
