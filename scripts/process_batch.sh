#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
if [[ $# -ge 1 ]]; then
  batch_id="$1"
  shift
else
  batch_id="$(python3 -c 'import json; print(json.load(open("data/generated/products.manifest.json"))["batch_id"])')"
fi
docker compose exec -T airflow python /opt/airflow/project/src/etl/process_batch.py --batch-id "$batch_id" "$@"
