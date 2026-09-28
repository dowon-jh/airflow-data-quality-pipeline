"""Pure, label-independent quality rules for a single RAW batch."""
from collections import Counter
from dataclasses import asdict, dataclass
from decimal import Decimal
import re
import unicodedata

RULESET_VERSION = "1.1"
CATEGORIES = frozenset({"TUMBLER", "BAG", "SHOES", "LAMP"})
PRODUCT_ID = re.compile(r"P[0-9]{6,}\Z")
QUANTITY = re.compile(
    r"(?<![\w.,+\-])([+-]?[0-9][0-9,.eE+\-]*)\s*(ml|mm|cm|kg|l|w|g)(?![A-Za-z0-9_])",
    re.IGNORECASE,
)
NUMBER = re.compile(r"[+-]?(?:[0-9]+|[0-9]{1,3}(?:,[0-9]{3})+)(?:\.[0-9]+)?\Z")


def normalize(value):
    """NFKC and whitespace collapse; preserve None separately from empty text."""
    return None if value is None else " ".join(unicodedata.normalize("NFKC", value).split())


def normalized_id(value):
    result = normalize(value)
    return result.upper() if result is not None else None


def parse_quantities(value):
    tokens, invalid = set(), []
    for number, unit in QUANTITY.findall(normalize(value) or ""):
        if NUMBER.fullmatch(number):
            tokens.add((Decimal(number.replace(",", "")), unit.casefold()))
        else:
            invalid.append(number + unit)
    return tokens, invalid


def quantities(value):
    return parse_quantities(value)[0]


@dataclass(frozen=True)
class Issue:
    rule_id: str
    error_type: str
    field_name: str
    severity: str
    error_message: str


def evaluate_batch(records):
    """Return ordered per-row decisions. Never mutate input or consult fault labels."""
    if not records:
        raise ValueError("Cannot evaluate an empty RAW batch")
    if len({row["batch_id"] for row in records}) != 1:
        raise ValueError("Quality engine expects exactly one batch")
    if len({row["source_record_id"] for row in records}) != len(records):
        raise ValueError("Duplicate source_record_id in RAW input")
    id_counts = Counter(normalized_id(row["product_id"]) for row in records
                        if normalized_id(row["product_id"]))
    results = []
    for row in records:
        issues = []

        def add(rule, name, field, message, severity="ERROR"):
            issues.append(asdict(Issue(rule, name, field, severity, message)))

        product_id = normalized_id(row["product_id"])
        category = normalize(row["category"])
        category = category.upper() if category is not None else None
        attribute = normalize(row["extracted_attribute"])
        original = normalize(row["original_text"])
        ocr = normalize(row["ocr_text"])
        if not product_id:
            add("DQ001", "PRODUCT_ID_REQUIRED", "product_id", "상품 ID가 NULL 또는 공백입니다.")
        elif not PRODUCT_ID.fullmatch(product_id):
            add("DQ002", "PRODUCT_ID_FORMAT", "product_id", "상품 ID는 P와 6자리 이상 숫자여야 합니다.")
        if product_id and id_counts[product_id] > 1:
            add("DQ003", "DUPLICATE_PRODUCT_ID", "product_id",
                f"정규화한 상품 ID가 배치 안에 {id_counts[product_id]}회 있습니다. 그룹 전체가 격리 대상입니다.")
        if category not in CATEGORIES:
            add("DQ004", "CATEGORY_ALLOWED", "category", "허용된 상품 카테고리가 아닙니다.")
        if attribute is None:
            add("DQ005", "ATTRIBUTE_REQUIRED", "extracted_attribute", "추출 속성이 NULL입니다.")
        elif not attribute:
            add("DQ006", "ATTRIBUTE_NOT_EMPTY", "extracted_attribute", "추출 속성이 빈 문자열 또는 공백입니다.")
        else:
            if not any(character.isalnum() for character in attribute):
                add("DQ007", "ATTRIBUTE_HAS_CONTENT", "extracted_attribute", "속성에 글자나 숫자가 없습니다.")
            if len(attribute) > 200:
                add("DQ008", "ATTRIBUTE_LENGTH_LIMIT", "extracted_attribute", "정규화한 속성이 200자를 초과합니다.")
        if (original or "").casefold() != (ocr or "").casefold():
            add("DQ009", "OCR_TEXT_MISMATCH_SIGNAL", "ocr_text",
                "정규화한 OCR과 원문이 다릅니다. 단순 불일치 신호이며 OCR 오류 확정은 아닙니다.", "WARNING")
        attribute_tokens, invalid_attribute = parse_quantities(attribute)
        original_tokens, invalid_original = parse_quantities(original)
        evidence_status = "CHECKED" if attribute_tokens else "NO_SUPPORTED_QUANTITY"
        if invalid_attribute or (attribute_tokens and invalid_original):
            evidence_status = "UNPARSEABLE"
            add("DQ010", "ATTRIBUTE_EVIDENCE_UNPARSEABLE", "extracted_attribute",
                "숫자·단위 표현을 안전하게 해석할 수 없어 근거 확인이 필요합니다.")
        unsupported = attribute_tokens - original_tokens
        if unsupported:
            evidence = ", ".join(f"{number}{unit}" for number, unit in sorted(unsupported))
            # Keep one issue per row/rule; unparseable evidence already explains DQ010.
            if evidence_status != "UNPARSEABLE":
                add("DQ010", "HALLUCINATION_CANDIDATE", "extracted_attribute",
                    f"원문에 같은 숫자·단위가 없는 후보: {evidence}. 의미적 환각 확정은 아닙니다.")
        results.append({
            "batch_id": row["batch_id"], "source_record_id": row["source_record_id"],
            "decision": "QUARANTINE" if any(i["severity"] == "ERROR" for i in issues) else "PASS",
            "evidence_status": evidence_status,
            "normalized": {"product_id": product_id, "category": category,
                           "extracted_attribute": attribute},
            "issues": issues,
        })
    return results
