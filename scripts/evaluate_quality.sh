#!/usr/bin/env bash
# Run from anywhere in Ubuntu WSL; atomically publish only a completed report.
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
if [[ $# -ge 1 ]]; then
  batch_id="$1"
else
  batch_id="$(python3 -c 'import json; print(json.load(open("data/generated/products.manifest.json"))["batch_id"])')"
fi
output="${2:-data/generated/quality-report.json}"
mkdir -p -- "$(dirname -- "$output")"
temporary="$(mktemp -- "${output}.tmp.XXXXXX")"
trap 'rm -f -- "$temporary"' EXIT
docker compose exec -T airflow python /opt/airflow/project/src/dq/evaluate_raw.py \
  --batch-id "$batch_id" > "$temporary"
mv -- "$temporary" "$output"
python3 - "$output" <<'PY'
import json
import sys
from pathlib import Path
report = json.loads(Path(sys.argv[1]).read_text())
print("Batch:", report["batch_id"])
print(json.dumps(report["summary"], ensure_ascii=False, indent=2))
print("Report:", Path(sys.argv[1]).resolve())
PY
