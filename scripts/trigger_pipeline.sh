#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
if [[ $# -gt 1 ]]; then
  echo "Usage: bash scripts/trigger_pipeline.sh [data-relative-input.jsonl]" >&2
  exit 2
fi
input_file="${1:-generated/products.jsonl}"
conf="$(python3 -c 'import json,sys; print(json.dumps({"input_file": sys.argv[1]}))' "$input_file")"
docker compose exec -T airflow airflow dags unpause ai_output_quality_pipeline
docker compose exec -T airflow airflow dags trigger ai_output_quality_pipeline --conf "$conf"
