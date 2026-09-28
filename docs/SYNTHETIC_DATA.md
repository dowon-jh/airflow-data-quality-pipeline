# Synthetic Data

Phase 2에서는 가상의 상품 설명과 OCR·속성 추출 결과를 JSON Lines(JSONL) 형식으로 생성합니다. 실제 회사 데이터나 LLM API를 사용하지 않습니다.

## WSL에서 실행

프로젝트 루트에서 실행합니다.

```bash
python3 src/generator/generate_product_data.py
```

기본값은 1,000행, 시드 42, 오류 주입률 4%, 그중 `extracted_attribute` NULL 주입률 1%, 모델 버전 `v1.0`, 시작일 `2026-09-01`(UTC)입니다. `--null-rate`는 전체 행 수 기준이며 `--error-rate`에 포함됩니다.

모델 변경 후 NULL이 늘어난 사례는 별도 출력 파일로 만들 수 있습니다.

```bash
python3 src/generator/generate_product_data.py \
  --rows 1000 --seed 43 \
  --error-rate 0.12 --null-rate 0.08 \
  --model-version v1.1 \
  --output data/generated/products-degraded.jsonl
```

`--rows`, `--seed`, `--error-rate`, `--null-rate`, `--model-version`, `--start-date`, `--batch-id`, `--output`을 바꿀 수 있습니다. 비율은 행 수에 곱한 뒤 가장 가까운 정수로 반올림합니다. 같은 생성기 버전과 입력 옵션은 같은 행과 배치 ID를 생성합니다. 다른 배치를 파일로 보존하려면 다른 `--output`을 지정하세요.

2026-09-28: 생성기 버전 1.1을 매니페스트와 자동 배치 ID에 포함했습니다. 이전 길이 초과 예시는 실제 180자였으며 현재는 240자로 수정했습니다. 기본 배치는 `batch-a2ebfbe5e814`입니다. 가상 추출 `model_version=v1.0`과 생성기 버전은 별개입니다. 기존 DB 배치를 덮어쓰지 않습니다.

## 출력 파일

기본 실행 시 다음 파일이 생깁니다.

| 파일 | 목적 |
|---|---|
| `data/generated/products.jsonl` | 향후 RAW 적재에 사용할 입력 데이터 |
| `data/generated/products.labels.jsonl` | 주입한 오류의 행 ID·유형·예상 DQ 규칙 ID |
| `data/generated/products.manifest.json` | 생성 옵션, 건수, 오류 분포, 데이터 SHA-256 |

`data/generated/`는 Git에서 제외됩니다. 저장소에는 30행의 작은 예시를 `data/sample/`에 포함합니다. 이 예시는 모든 초기 오류 유형을 눈으로 확인하기 위해 오류 비율을 50%로 높였으므로 정상 배치 기준으로 사용하지 않습니다.

## 데이터 필드

각 JSONL 행에는 `source_record_id`, `batch_id`, `product_id`, `category`, `original_text`, `ocr_text`, `extracted_attribute`, `attribute_type`, `model_version`, `created_at`이 있습니다. `created_at`은 UTC의 ISO 8601 문자열입니다. `ingested_at`은 Phase 3에서 PostgreSQL RAW 적재 시 부여합니다.

JSONL은 `null`과 공백 문자열을 구별합니다. 예를 들어 NULL 오류는 `"extracted_attribute":null`, 빈 값 오류는 `"extracted_attribute":"   "`로 생성됩니다.

## 주입 오류와 규칙 대응

| 주입 유형 | 의도한 규칙 | 데이터 변화 |
|---|---|---|
| `NULL_PRODUCT_ID` | DQ001 | `product_id`를 NULL로 변경 |
| `INVALID_PRODUCT_ID` | DQ002 | ID 접두어를 `X`로 변경 |
| `DUPLICATE_PRODUCT_ID` | DQ003 | 다른 행의 유효한 `product_id` 복사 |
| `INVALID_CATEGORY` | DQ004 | 카테고리를 `UNKNOWN`으로 변경 |
| `NULL_ATTRIBUTE` | DQ005 | 추출 속성을 NULL로 변경 |
| `EMPTY_ATTRIBUTE` | DQ006 | 추출 속성을 공백 문자열로 변경 |
| `SPECIAL_CHARACTER_ONLY` | DQ007 | 추출 속성을 특수문자만 있는 값으로 변경 |
| `EXCESSIVE_LENGTH` | DQ008 | 추출 속성을 200자 넘게 변경 |
| `OCR_MISMATCH` | DQ009 | OCR 텍스트의 숫자·단위를 원문과 다르게 변경 |
| `HALLUCINATION_CANDIDATE` | DQ010 | 원문에 없는 숫자·단위 속성으로 변경 |

생성기는 한 입력 행에 한 유형의 오류만 주입합니다. 다만 중복 ID를 복사받은 원래 행도 DQ003 검사에서는 중복 그룹으로 분류될 수 있습니다. 따라서 매니페스트의 `normal_count`는 **오류를 주입하지 않은 행 수**이며 향후 CLEAN의 최종 통과 건수와 반드시 같지는 않습니다. DQ009는 차이 신호이고 DQ010은 추가 검수 후보이므로 라벨을 확정적인 OCR 오류나 환각 정답으로 해석하지 않습니다.

라벨 파일은 생성기 검증과 향후 규칙 평가에만 사용합니다. RAW 적재나 DAG 판정에 입력하지 않습니다. 원문과 추출 결과만 보고 판단하도록 분리해 둡니다.
