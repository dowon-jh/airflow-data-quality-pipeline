# Data Model

## 기준

- 스키마는 PostgreSQL 내부 schema와 테이블로 구분합니다.
- `product_id`는 상품 식별자입니다. 같은 ID가 들어오는 상황도 품질 검사 대상이므로 RAW에는 중복 입력을 보존할 수 있어야 합니다. 원천 행은 `(batch_id, source_record_id)`로 식별합니다.
- 처리 배치는 `batch_id`로 식별합니다. RAW 원본 행마다 `source_record_id`를 두어 중복 `product_id`와 구분합니다.
- 시간은 PostgreSQL `TIMESTAMPTZ`로 저장하고 UTC 기준으로 기록합니다.

## RAW: `raw.ai_product_output`

| 컬럼 | 타입 초안 | 의미 |
|---|---|---|
| `source_record_id` | BIGINT | 배치 안에서 1부터 시작하는 입력 행 식별자 |
| `batch_id` | TEXT | 생성 옵션에서 결정되는 배치 식별자 |
| `product_id` | TEXT, nullable | 상품 식별자; 결측·중복 검사 대상 |
| `category` | TEXT, nullable | 상품 카테고리 |
| `original_text` | TEXT, nullable | 기준이 되는 가상 원문 |
| `ocr_text` | TEXT, nullable | 가상 OCR 결과 |
| `extracted_attribute` | TEXT, nullable | 추출된 속성 값 |
| `attribute_type` | TEXT, nullable | 속성 유형 |
| `model_version` | TEXT, nullable | 가상 생성 모델 버전 |
| `created_at` | TIMESTAMPTZ, nullable | 원천 생성 시각 |
| `ingested_at` | TIMESTAMPTZ | RAW 적재 시각 |

RAW는 입력을 그대로 보존합니다. 품질 오류를 적재 시점에 거부하지 않도록 업무 필드의 NULL 허용 여부는 검증 단계에서 적용합니다. 적재 메타데이터의 타입과 필수 여부는 구현 시 확정합니다.

Phase 2 생성 파일에는 `ingested_at`이 없습니다. Phase 3 RAW 적재 시 DB에서 기록합니다. `(batch_id, source_record_id)`로 입력 행을 고유하게 식별합니다.

Phase 3의 실제 DDL은 `sql/schemas/001_create_layers.sql`과 `sql/raw/002_create_raw_tables.sql`에 있습니다. `product_id`에 고유 제약을 두지 않아 원본의 중복을 보존합니다. `raw.ingest_batch`에는 `batch_id`, 파일 SHA-256, 행 수, 파일명, 적재 시각을 기록합니다. 같은 배치가 다시 들어오면 해시와 행 수를 비교합니다.

## CLEAN: `clean.ai_product_output`

검증 및 정규화를 통과한 레코드입니다. RAW의 식별·업무 필드를 유지하고 다음 처리 컬럼을 추가합니다.

| 컬럼 | 의미 |
|---|---|
| `batch_id` | 원본 처리 배치 |
| `quality_status` | 첫 버전에서 `PASS` |
| `processed_at` | 처리 시각 |
| `dag_run_id` | 처리한 Airflow 실행 식별자 |
| `processing_run_id` | 수동·DAG 실행에 공통으로 부여하는 UUID |
| `ruleset_version`, `evidence_status` | 규칙 버전과 숫자 근거 검사 상태 |

같은 `batch_id`와 `source_record_id` 조합은 재실행 시 중복되지 않아야 합니다.

Phase 5 실제 DDL은 `sql/etl/006_create_result_tables.sql`입니다. 최신 PASS 행만 저장하고 `(batch_id, product_id)`도 고유합니다. 상품 ID·카테고리·추출 속성은 정규화하고 원문·OCR·기타 원천 업무 값·`ingested_at`은 유지합니다.

## QUARANTINE: `quarantine.ai_product_output`

오류를 포함하는 입력 레코드를 원본 값과 함께 보관합니다. 한 레코드가 여러 규칙에 걸릴 수 있으므로 오류 상세는 별도 자식 테이블로 정규화하는 방안을 기본으로 합니다.

### `quarantine.ai_product_output`

ERROR가 있는 레코드의 원본 필드, `batch_id`, `source_record_id`, `detected_at`, `dag_run_id`, `processing_run_id`, `ruleset_version`, `evidence_status`, `quality_status=QUARANTINE`을 보관합니다. `(batch_id, source_record_id)`로 재실행 중복을 방지합니다. 업무 필드와 `ingested_at`은 RAW 그대로 저장합니다.

### `quarantine.dq_issue`

| 컬럼 | 의미 |
|---|---|
| `batch_id`, `source_record_id` | RAW 입력 레코드 참조; 통과 행의 WARNING도 보존 가능하게 설계 |
| `rule_id` | 예: `DQ005` |
| `error_type` | 기계 판독 가능한 오류명 |
| `error_message` | 사람이 확인할 설명 |
| `field_name` | 문제가 발견된 필드(해당 시) |
| `severity` | ERROR 또는 WARNING; ERROR가 있을 때만 행 격리 |
| `ruleset_version` | 적용한 규칙 버전 |
| `detected_at` | 탐지 시각 |

한 레코드의 여러 이슈를 모두 기록합니다. Phase 5 PK는 `(batch_id, source_record_id, rule_id)`이며 `processing_run_id`, `dag_run_id`도 저장합니다. 결과 두 테이블과 이슈는 RAW 행 및 처리 실행 FK로 연결합니다.

Phase 4는 JSON 보고서를 생성하며 Phase 5는 위 결과 테이블에 실제 적재합니다. DQ009 WARNING만 있는 행은 PASS이므로 이슈 FK는 RAW를 참조합니다. schema 이름이 `quarantine`이어도 해당 이슈 테이블은 통과 행의 경고까지 담습니다.

## 처리 이력: `clean.processing_run`

성공 실행별 `processing_run_id` UUID, `batch_id`, 규칙 버전, 선택적 DAG ID, 입력 해시 메타데이터, 입력·CLEAN·QUARANTINE·WARNING·이슈 건수, 규칙별 JSON 집계, `completed_at`, `is_current`를 보관합니다. 배치당 `is_current=true`는 하나입니다.

현재 결과·이슈는 최신 실행만 저장하고 성공 요약 이력은 누적합니다. 전체 과거 행 snapshot은 저장하지 않습니다. 같은 현재 UUID 재시도는 추가 저장 없이 반환합니다. 새 UUID는 결과를 한 트랜잭션으로 교체하고 이력을 추가합니다. 실패한 트랜잭션은 성공 이력에 남기지 않습니다. 상세 정책: [Phase 5 구현 기록](PHASE5_SPLIT_LOAD.md).

## MART: `mart.daily_quality_metrics`

Phase 7 실제 DDL은 `sql/mart/008_create_quality_mart.sql`입니다. 차원은 `metric_date`, `category`, `model_version`, `batch_id`이며 입력·통과·격리·경고·NULL 속성·중복 ID·근거 후보·생성 시각 결측 행 수와 이슈 수를 저장합니다. 통과율·격리율·NULL 속성 비율은 NUMERIC generated 컬럼입니다. 저장 그룹은 입력 1행 이상이며 0행 그룹은 만들지 않습니다.

`metric_id` BIGSERIAL PK와 `UNIQUE NULLS NOT DISTINCT (batch_id,metric_date,category,model_version)`으로 NULL 차원 중복도 막습니다. 처리 UUID·배치는 `clean.processing_run` FK로 연결합니다. 날짜는 RAW 생성 시각 UTC, 결측 시 RAW 적재일 UTC입니다. 카테고리는 정규화 대문자, 모델 버전은 대소문자 유지 정규화이며 NULL·빈 문자열·UNKNOWN을 보존합니다.

`mart.daily_rule_metrics`는 `(metric_id,rule_id,severity)` PK로 규칙별 이슈 수를 저장합니다. 부모 마트 삭제 시 cascade 삭제합니다. 마트는 배치 최신 결과만 저장하며 새 분리 적재가 이전 마트를 같은 트랜잭션으로 지웁니다. `mart.current_daily_quality_metrics` 뷰는 현재 처리 실행의 마트만 제공합니다.

## 게이트 이력: `mart.batch_quality_gate`

`(processing_run_id,policy_version)` PK로 배치·실제 DAG ID·입력/격리 행·임계치·실제 비율·PASS/FAIL·판정 시각을 보관합니다. 처리 실행 FK로 연결하며 새 run의 과거 판정을 삭제하지 않습니다. 정책 1.0은 배치 전체 격리율 > 5%이면 FAIL입니다. 정확히 5%는 PASS이며 그룹별 게이트는 아닙니다. 판정 커밋 이후 DAG 태스크 실패를 표시해 실패 증거를 보존합니다. 과거 전체 마트 snapshot은 저장하지 않습니다. [Phase 7 정책·실행 기록](PHASE7_QUALITY_MART.md)을 참고하세요.

## 초기 용어 정의

- **입력 행 수:** 해당 배치에서 RAW에 보존된 행 수
- **통과 행 수:** 레코드 수준 DQ 오류가 없고 CLEAN에 적재된 행 수
- **격리 행 수:** 하나 이상의 레코드 수준 DQ 오류가 있어 QUARANTINE에 적재된 고유 입력 행 수
- **규칙별 오류 건수:** 특정 `rule_id`에 기록된 이슈 수; 한 레코드가 여러 규칙에 걸리면 각각 계산
- **통과율:** 통과 행 수 / 입력 행 수

이 정의를 지표 계산과 README에서 일관되게 사용합니다.
