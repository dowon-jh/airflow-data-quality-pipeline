"""Read one committed RAW batch and write a JSON quality report to stdout."""
import argparse
from datetime import datetime, timezone
import json
import sys

import psycopg2
from psycopg2.extras import RealDictCursor
from common.db import connect_project_db
from dq.engine import RULESET_VERSION, evaluate_batch
from dq.raw import read_raw_batch, summarize_results


def evaluate_raw(batch_id):
    connection = connect_project_db()
    try:
        connection.set_session(isolation_level="REPEATABLE READ", readonly=True)
        with connection:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                manifest, records = read_raw_batch(cursor, batch_id)
        results = evaluate_batch(records)
        return {
            "batch_id": batch_id, "ruleset_version": RULESET_VERSION,
            "evaluated_at": datetime.now(timezone.utc).isoformat(),
            "source_data_sha256": manifest["data_sha256"],
            "summary": summarize_results(results),
            "records": results,
        }
    finally:
        connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-id", required=True)
    args = parser.parse_args()
    try:
        report = evaluate_raw(args.batch_id)
    except (OSError, ValueError, KeyError, psycopg2.Error) as exc:
        print(f"Quality evaluation failed: {exc}", file=sys.stderr)
        return 1
    json.dump(report, sys.stdout, ensure_ascii=False, indent=2)
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
