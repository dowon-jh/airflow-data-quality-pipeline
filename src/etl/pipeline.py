"""Small metadata contracts for the Airflow tasks; business rows stay out of XCom."""
import logging
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from dq.evaluate_raw import evaluate_raw
from etl.process_batch import process_batch
from load.load_raw import inspect_input, load_batch, verify_manifest

DATA_ROOT = Path("/opt/airflow/project/data")
LOGGER = logging.getLogger(__name__)


def collect_input(input_file):
    root = DATA_ROOT.resolve()
    relative = Path(input_file)
    if relative.is_absolute():
        raise ValueError("input_file must be a relative path inside the project data directory")
    path = (root / relative).resolve()
    if not path.is_relative_to(root) or path.suffix != ".jsonl" or path.name.endswith(".labels.jsonl"):
        raise ValueError("input_file must be an input JSONL inside the project data directory")
    batch_id, rows, digest = inspect_input(path)
    verify_manifest(path, batch_id, rows, digest)
    LOGGER.info("Input collected: batch=%s rows=%s sha256=%s", batch_id, rows, digest)
    return {"input_file": path.relative_to(root).as_posix(), "batch_id": batch_id,
            "rows": rows, "data_sha256": digest}


def ingest_input(source):
    current = collect_input(source["input_file"])
    if current != source:
        raise ValueError("Input changed after collection; trigger a new run with a stable input file")
    message = load_batch(DATA_ROOT / source["input_file"], chunk_size=1000,
                         expected_input=(source["batch_id"], source["rows"], source["data_sha256"]))
    LOGGER.info("%s", message)
    # Bind committed RAW metadata to the collected identity even if the file changed in between reads.
    from common.db import connect_project_db
    connection = connect_project_db()
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT row_count, data_sha256 FROM raw.ingest_batch WHERE batch_id=%s",
                               (source["batch_id"],))
                if cursor.fetchone() != (source["rows"], source["data_sha256"]):
                    raise ValueError("Committed RAW does not match collected input")
    finally:
        connection.close()
    return source


def evaluate_input(source):
    report = evaluate_raw(source["batch_id"])
    if (report["source_data_sha256"] != source["data_sha256"]
            or report["summary"]["input_rows"] != source["rows"]):
        raise ValueError("RAW quality report does not match collected input")
    quality = {key: report[key] for key in ("batch_id", "ruleset_version", "source_data_sha256", "summary")}
    LOGGER.info("Quality evaluated: batch=%s summary=%s", source["batch_id"], quality["summary"])
    return quality


def persist_results(quality, dag_id, dag_run_id):
    # Stable across task retry/clear in the same DAG run, distinct for a new DAG run.
    run_id = str(uuid5(NAMESPACE_URL, f"{dag_id}/{dag_run_id}/{quality['batch_id']}"))
    outcome = process_batch(quality["batch_id"], processing_run_id=run_id,
                            dag_run_id=dag_run_id, expected_quality=quality)
    LOGGER.info("Results stored: dag_run_id=%s processing_run_id=%s status=%s summary=%s",
                dag_run_id, run_id, outcome["status"], outcome["summary"])
    return outcome
