"""Load a generated JSONL batch into PostgreSQL RAW without changing its values."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values
from common.db import connect_project_db


FIELDS = (
    "source_record_id",
    "batch_id",
    "product_id",
    "category",
    "original_text",
    "ocr_text",
    "extracted_attribute",
    "attribute_type",
    "model_version",
    "created_at",
)
NULLABLE_TEXT_FIELDS = (
    "product_id",
    "category",
    "original_text",
    "ocr_text",
    "extracted_attribute",
    "attribute_type",
    "model_version",
)
INSERT_RAW = """
    INSERT INTO raw.ai_product_output (
        source_record_id, batch_id, product_id, category, original_text,
        ocr_text, extracted_attribute, attribute_type, model_version, created_at
    ) VALUES %s
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True, help="Generated products.jsonl")
    parser.add_argument("--chunk-size", type=int, default=1000)
    args = parser.parse_args()
    if args.chunk_size < 1:
        parser.error("--chunk-size must be positive")
    return args


def validate_record(record: object, line_number: int) -> dict[str, object]:
    if not isinstance(record, dict):
        raise ValueError(f"Line {line_number}: expected a JSON object")
    if set(record) != set(FIELDS):
        missing = sorted(set(FIELDS) - set(record))
        extra = sorted(set(record) - set(FIELDS))
        raise ValueError(f"Line {line_number}: missing fields {missing}; extra fields {extra}")
    source_id = record["source_record_id"]
    if isinstance(source_id, bool) or not isinstance(source_id, int) or source_id < 1:
        raise ValueError(f"Line {line_number}: source_record_id must be a positive integer")
    if not isinstance(record["batch_id"], str) or not record["batch_id"]:
        raise ValueError(f"Line {line_number}: batch_id must be a nonempty string")
    for field in NULLABLE_TEXT_FIELDS:
        if record[field] is not None and not isinstance(record[field], str):
            raise ValueError(f"Line {line_number}: {field} must be text or null")
    created_at = record["created_at"]
    if created_at is not None:
        if not isinstance(created_at, str):
            raise ValueError(f"Line {line_number}: created_at must be text or null")
        try:
            parsed = datetime.fromisoformat(created_at)
        except ValueError as exc:
            raise ValueError(f"Line {line_number}: invalid created_at") from exc
        if parsed.tzinfo is None:
            raise ValueError(f"Line {line_number}: created_at needs a timezone")
    return record


def inspect_input(path: Path) -> tuple[str, int, str]:
    digest = hashlib.sha256()
    batch_id: str | None = None
    source_ids: set[int] = set()
    rows = 0
    with path.open("rb") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            digest.update(raw_line)
            if not raw_line.strip():
                raise ValueError(f"Line {line_number}: empty JSONL line")
            try:
                record = validate_record(json.loads(raw_line), line_number)
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                raise ValueError(f"Line {line_number}: invalid UTF-8 JSON") from exc
            current_batch = record["batch_id"]
            if batch_id is None:
                batch_id = current_batch
            elif current_batch != batch_id:
                raise ValueError(f"Line {line_number}: multiple batch_id values in one input")
            source_id = record["source_record_id"]
            if source_id in source_ids:
                raise ValueError(f"Line {line_number}: duplicate source_record_id {source_id}")
            source_ids.add(source_id)
            rows += 1
    if batch_id is None:
        raise ValueError("Input JSONL is empty")
    return batch_id, rows, digest.hexdigest()


def verify_manifest(path: Path, batch_id: str, rows: int, digest: str) -> None:
    manifest_path = path.with_name(f"{path.stem}.manifest.json")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        raise ValueError(f"Missing or invalid manifest: {manifest_path}") from exc
    expected = {
        "batch_id": batch_id,
        "rows": rows,
        "data_sha256": digest,
    }
    mismatches = [key for key, value in expected.items() if manifest.get(key) != value]
    if mismatches:
        raise ValueError(f"Manifest mismatch: {', '.join(mismatches)}")


def iter_rows(path: Path, digest):
    with path.open("rb") as handle:
        for line in handle:
            digest.update(line)
            record = json.loads(line)
            yield tuple(record[field] for field in FIELDS)


def load_batch(path: Path, chunk_size: int, expected_input=None) -> str:
    batch_id, expected_rows, digest = inspect_input(path)
    verify_manifest(path, batch_id, expected_rows, digest)
    if expected_input is not None and (batch_id, expected_rows, digest) != expected_input:
        raise ValueError("Input changed before RAW loading")
    connection = connect_project_db()
    try:
        with connection:
            with connection.cursor() as cursor:
                # Same-batch serialization without requiring RAW UPDATE rights.
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (batch_id,)
                )
                cursor.execute(
                    "SELECT data_sha256, row_count FROM raw.ingest_batch WHERE batch_id = %s",
                    (batch_id,),
                )
                previous = cursor.fetchone()
                if previous:
                    cursor.execute(
                        "SELECT COUNT(*) FROM raw.ai_product_output WHERE batch_id = %s",
                        (batch_id,),
                    )
                    stored_rows = cursor.fetchone()[0]
                    if previous != (digest, expected_rows) or stored_rows != expected_rows:
                        raise ValueError(
                            f"Batch {batch_id} already exists with different content or row count"
                        )
                    return f"Batch {batch_id} already loaded: {stored_rows} rows"

                chunk: list[tuple[object, ...]] = []
                loaded_digest = hashlib.sha256()
                for row in iter_rows(path, loaded_digest):
                    chunk.append(row)
                    if len(chunk) >= chunk_size:
                        execute_values(cursor, INSERT_RAW, chunk, page_size=chunk_size)
                        chunk.clear()
                if chunk:
                    execute_values(cursor, INSERT_RAW, chunk, page_size=chunk_size)
                if loaded_digest.hexdigest() != digest:
                    raise ValueError("Input changed between validation and loading")

                cursor.execute(
                    "SELECT COUNT(*) FROM raw.ai_product_output WHERE batch_id = %s",
                    (batch_id,),
                )
                loaded_rows = cursor.fetchone()[0]
                if loaded_rows != expected_rows:
                    raise ValueError(
                        f"Batch {batch_id}: expected {expected_rows} rows, found {loaded_rows}"
                    )
                cursor.execute(
                    """
                    INSERT INTO raw.ingest_batch (batch_id, data_sha256, row_count, source_file)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (batch_id, digest, expected_rows, path.name),
                )
        return f"Loaded batch {batch_id}: {expected_rows} rows"
    finally:
        connection.close()


def main() -> None:
    args = parse_args()
    try:
        message = load_batch(args.input, args.chunk_size)
    except (OSError, ValueError, psycopg2.Error) as exc:
        raise SystemExit(f"RAW load failed: {exc}") from exc
    print(message)


if __name__ == "__main__":
    main()
