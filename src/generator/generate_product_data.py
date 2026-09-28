"""Generate reproducible synthetic OCR/product extraction data.

The JSONL data file models RAW input. Injected-fault labels are written to a
separate file so the pipeline cannot read the expected answer from RAW.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import tempfile
from collections import Counter
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path


GENERATOR_VERSION = "1.1"

TEMPLATES = (
    {
        "category": "TUMBLER",
        "original_text": "스테인리스 텀블러 500ml, 보온 12시간",
        "attribute": "500ml",
        "candidate_attribute": "900ml",
        "attribute_type": "CAPACITY",
    },
    {
        "category": "BAG",
        "original_text": "생활방수 백팩 20L, 노트북 수납 가능",
        "attribute": "20L",
        "candidate_attribute": "35L",
        "attribute_type": "CAPACITY",
    },
    {
        "category": "SHOES",
        "original_text": "러닝화 250mm, 경량 메시 소재",
        "attribute": "250mm",
        "candidate_attribute": "280mm",
        "attribute_type": "SIZE",
    },
    {
        "category": "LAMP",
        "original_text": "LED 스탠드 12W, 밝기 3단계",
        "attribute": "12W",
        "candidate_attribute": "24W",
        "attribute_type": "POWER",
    },
)

NULL_ERROR = "NULL_ATTRIBUTE"
OTHER_ERRORS = (
    "NULL_PRODUCT_ID",
    "INVALID_PRODUCT_ID",
    "DUPLICATE_PRODUCT_ID",
    "INVALID_CATEGORY",
    "EMPTY_ATTRIBUTE",
    "SPECIAL_CHARACTER_ONLY",
    "EXCESSIVE_LENGTH",
    "OCR_MISMATCH",
    "HALLUCINATION_CANDIDATE",
)
ERROR_RULE_IDS = {
    "NULL_PRODUCT_ID": "DQ001",
    "INVALID_PRODUCT_ID": "DQ002",
    "DUPLICATE_PRODUCT_ID": "DQ003",
    "INVALID_CATEGORY": "DQ004",
    "NULL_ATTRIBUTE": "DQ005",
    "EMPTY_ATTRIBUTE": "DQ006",
    "SPECIAL_CHARACTER_ONLY": "DQ007",
    "EXCESSIVE_LENGTH": "DQ008",
    "OCR_MISMATCH": "DQ009",
    "HALLUCINATION_CANDIDATE": "DQ010",
}


def rate_count(rows: int, rate: Decimal) -> int:
    return int((Decimal(rows) * rate).to_integral_value(rounding=ROUND_HALF_UP))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=1000, help="Number of records (default: 1000)")
    parser.add_argument("--seed", type=int, default=42, help="Deterministic random seed")
    parser.add_argument(
        "--error-rate",
        type=Decimal,
        default=Decimal("0.04"),
        help="Fraction of records with one injected fault (default: 0.04)",
    )
    parser.add_argument(
        "--null-rate",
        type=Decimal,
        default=Decimal("0.01"),
        help="Fraction with NULL extracted_attribute, included in error-rate (default: 0.01)",
    )
    parser.add_argument("--model-version", default="v1.0")
    parser.add_argument("--start-date", default="2026-09-01", help="UTC date, YYYY-MM-DD")
    parser.add_argument("--batch-id", help="Optional batch ID; otherwise derived from parameters")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/generated/products.jsonl"),
        help="Output JSONL path",
    )
    args = parser.parse_args()

    if args.rows < 2:
        parser.error("--rows must be at least 2")
    if not all(rate.is_finite() for rate in (args.error_rate, args.null_rate)):
        parser.error("rates must be finite decimals")
    if not (Decimal(0) <= args.null_rate <= args.error_rate <= Decimal(1)):
        parser.error("rates must satisfy 0 <= null-rate <= error-rate <= 1")
    try:
        args.start_day = date.fromisoformat(args.start_date)
    except ValueError:
        parser.error("--start-date must be a valid YYYY-MM-DD date")
    if args.output.suffix != ".jsonl":
        parser.error("--output must end in .jsonl")
    if not args.model_version.strip():
        parser.error("--model-version cannot be empty")
    if args.batch_id is not None and (
        not args.batch_id or
        len(args.batch_id) > 80 or
        not all(c.isascii() and (c.isalnum() or c in "_-") for c in args.batch_id)
    ):
        parser.error("--batch-id must contain 1-80 ASCII letters, digits, _ or -")
    return args


def build_batch_id(args: argparse.Namespace) -> str:
    if args.batch_id:
        return args.batch_id
    settings = {
        "generator_version": GENERATOR_VERSION,
        "rows": args.rows,
        "seed": args.seed,
        "error_rate": str(args.error_rate),
        "null_rate": str(args.null_rate),
        "model_version": args.model_version,
        "start_date": args.start_date,
    }
    digest = hashlib.sha256(
        json.dumps(settings, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()[:12]
    return f"batch-{digest}"


def schedule_faults(args: argparse.Namespace, rng: random.Random) -> dict[int, str]:
    total = rate_count(args.rows, args.error_rate)
    nulls = rate_count(args.rows, args.null_rate)
    if nulls > total:
        raise ValueError("Rounded NULL count exceeds total error count; increase --rows")

    faults = [NULL_ERROR] * nulls
    faults.extend(OTHER_ERRORS[i % len(OTHER_ERRORS)] for i in range(total - nulls))
    rng.shuffle(faults)
    positions = rng.sample(range(args.rows), total)
    return dict(zip(positions, faults))


def make_record(
    index: int,
    batch_id: str,
    model_version: str,
    start_at: datetime,
    fault: str | None,
    duplicate_source: int | None,
    rng: random.Random,
) -> dict[str, object]:
    template = rng.choice(TEMPLATES)
    record: dict[str, object] = {
        "source_record_id": index + 1,
        "batch_id": batch_id,
        "product_id": f"P{index + 1:06d}",
        "category": template["category"],
        "original_text": template["original_text"],
        "ocr_text": template["original_text"],
        "extracted_attribute": template["attribute"],
        "attribute_type": template["attribute_type"],
        "model_version": model_version,
        "created_at": (start_at + timedelta(seconds=index)).isoformat(),
    }

    if fault == "NULL_PRODUCT_ID":
        record["product_id"] = None
    elif fault == "INVALID_PRODUCT_ID":
        record["product_id"] = f"X{index + 1:06d}"
    elif fault == "DUPLICATE_PRODUCT_ID":
        assert duplicate_source is not None
        record["product_id"] = f"P{duplicate_source + 1:06d}"
    elif fault == "INVALID_CATEGORY":
        record["category"] = "UNKNOWN"
    elif fault == NULL_ERROR:
        record["extracted_attribute"] = None
    elif fault == "EMPTY_ATTRIBUTE":
        record["extracted_attribute"] = "   "
    elif fault == "SPECIAL_CHARACTER_ONLY":
        record["extracted_attribute"] = "!@#$%"
    elif fault == "EXCESSIVE_LENGTH":
        record["extracted_attribute"] = "길이초과" * 60
    elif fault == "OCR_MISMATCH":
        record["ocr_text"] = template["original_text"].replace(
            template["attribute"], template["candidate_attribute"], 1
        )
    elif fault == "HALLUCINATION_CANDIDATE":
        record["extracted_attribute"] = template["candidate_attribute"]
    return record


def write_json_line(handle, value: dict[str, object], digest=None) -> None:
    line = json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n"
    handle.write(line)
    if digest is not None:
        digest.update(line.encode("utf-8"))


def generate(args: argparse.Namespace) -> tuple[Path, Path, Path, dict[str, object]]:
    rng = random.Random(args.seed)
    faults = schedule_faults(args, rng)
    valid_id_positions = [
        i for i in range(args.rows)
        if faults.get(i) not in {"NULL_PRODUCT_ID", "INVALID_PRODUCT_ID", "DUPLICATE_PRODUCT_ID"}
    ]
    if not valid_id_positions and "DUPLICATE_PRODUCT_ID" in faults.values():
        raise ValueError("No valid product ID is available for duplicate injection")

    batch_id = build_batch_id(args)
    start_at = datetime.combine(args.start_day, time.min, tzinfo=timezone.utc)
    output = args.output
    labels = output.with_name(f"{output.stem}.labels.jsonl")
    manifest_path = output.with_name(f"{output.stem}.manifest.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    counts: Counter[str] = Counter(faults.values())
    data_digest = hashlib.sha256()

    with tempfile.TemporaryDirectory(dir=output.parent) as temp_dir:
        temp_path = Path(temp_dir)
        data_temp = temp_path / output.name
        labels_temp = temp_path / labels.name
        manifest_temp = temp_path / manifest_path.name
        with data_temp.open("w", encoding="utf-8", newline="\n") as data_file, labels_temp.open(
            "w", encoding="utf-8", newline="\n"
        ) as labels_file:
            for index in range(args.rows):
                fault = faults.get(index)
                partner = rng.choice(valid_id_positions) if fault == "DUPLICATE_PRODUCT_ID" else None
                record = make_record(
                    index, batch_id, args.model_version, start_at, fault, partner, rng
                )
                write_json_line(data_file, record, data_digest)
                if fault:
                    write_json_line(
                        labels_file,
                        {
                            "batch_id": batch_id,
                            "source_record_id": index + 1,
                            "injected_error_type": fault,
                            "expected_rule_id": ERROR_RULE_IDS[fault],
                        },
                    )

        manifest: dict[str, object] = {
            "generator_version": GENERATOR_VERSION,
            "batch_id": batch_id,
            "rows": args.rows,
            "seed": args.seed,
            "error_rate_requested": str(args.error_rate),
            "null_rate_requested": str(args.null_rate),
            "model_version": args.model_version,
            "start_date_utc": args.start_date,
            "normal_count": args.rows - len(faults),
            "injected_error_count": len(faults),
            "injected_error_counts": dict(sorted(counts.items())),
            "data_sha256": data_digest.hexdigest(),
        }
        manifest_temp.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(data_temp, output)
        os.replace(labels_temp, labels)
        os.replace(manifest_temp, manifest_path)
    return output, labels, manifest_path, manifest


def main() -> None:
    args = parse_args()
    try:
        output, labels, manifest_path, manifest = generate(args)
    except ValueError as exc:
        raise SystemExit(f"Generation failed: {exc}") from exc
    print(f"Data:     {output}")
    print(f"Labels:   {labels}")
    print(f"Manifest: {manifest_path}")
    print(
        f"Batch {manifest['batch_id']}: {manifest['rows']} rows, "
        f"{manifest['injected_error_count']} injected faults"
    )


if __name__ == "__main__":
    main()
