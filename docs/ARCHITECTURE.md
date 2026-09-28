# Architecture

## 목적과 범위

가상 상품 추출 데이터를 수동 배치로 처리하는 단일 머신용 학습 파이프라인입니다. 첫 버전의 품질 판단은 명시적인 Python/SQL 규칙으로 수행합니다. 외부 LLM 호출, 대시보드, 알림 서비스는 범위 밖입니다. Phase 0~9 기본 버전 완료 기준이며 실행 절차는 [RUNBOOK](RUNBOOK.md)에 있습니다.

## 구성 요소

| 구성 요소 | 책임 |
|---|---|
| Synthetic generator | 재현 가능한 정상·오류 샘플 생성 |
| PostgreSQL | 입력과 처리 결과, 품질 이력 저장 |
| Airflow scheduler/executor | 작업 의존성, 예약, 재시도와 실행 상태 관리 |
| Python/SQL tasks | 검증, 정제, 적재와 집계 |
| Docker Compose | 로컬 개발 환경 실행 |

Airflow는 데이터를 저장하거나 변환 로직을 모두 직접 수행하는 시스템이 아니라, 작업을 순서대로 실행하고 상태를 기록하는 오케스트레이터로 사용합니다.

## 논리 흐름

```text
생성 데이터
    │
    ▼
raw.ai_product_output ── 입력 보존
    │
    ▼
스키마 확인 → 정규화 → DQ 규칙 적용
    │                         │
    │ 통과                    │ 레코드 오류
    ▼                         ▼
clean.ai_product_output   quarantine.ai_product_output
    └──────────────┬──────────┘
                   ▼
        mart.daily_quality_metrics
                   ▼
          배치 임계치 확인
```

현재 실제 DAG는 `ai_output_quality_pipeline`입니다.

1. `collect_input`: 준비된 입력 경로·구조·manifest·해시 확인.
2. `load_raw`: 수집 identity 확인 및 RAW 멱등 적재.
3. `evaluate_quality`: 정규화·규칙 적용 후 작은 품질 요약 반환.
4. `load_results`: 같은 RAW 재평가·요약 비교 후 결과 원자적 교체.
5. `build_quality_metrics`: 현재 실행 결과를 원천 UTC 날짜·카테고리·모델별 집계.
6. `check_batch_thresholds`: 배치 전체 격리율 > 5%이면 판정 커밋 후 실패 표시.

입력 생성은 WSL에서 별도로 수행합니다. DAG는 수동 실행(`schedule=None`), `catchup=False`, `max_active_runs=1`입니다. 코드·입력 read-only mount와 제한 DB 계정을 유지합니다. 자세한 경계와 재시도 정책은 [Phase 6](PHASE6_AIRFLOW_DAG.md), 마트·정책 1.0은 [Phase 7](PHASE7_QUALITY_MART.md)에 있습니다. 최신 마트는 결과 교체 시 함께 무효화하고 게이트 판정 이력은 보존합니다.

## 처리 보장과 설계 기준

- RAW 입력은 원본 추적을 위해 수정하지 않습니다.
- 모든 실행은 `batch_id`로 식별하고 적재 결과에 DAG Run 식별자를 남깁니다.
- CLEAN 및 QUARANTINE 적재는 같은 배치를 재실행해도 중복되지 않도록 고유 키와 적재 전략을 사용합니다.
- 대량 행 데이터는 XCom으로 전달하지 않습니다. XCom에는 `batch_id`, 행 수, 상태 등 작은 메타데이터만 둡니다.
- 각 단계는 로그에 배치 ID, 입력·출력 건수와 실패 이유를 남깁니다.
- 비밀 정보는 저장소에 커밋하지 않고 환경 설정으로 주입합니다.

## 실패 경계

레코드 품질 오류는 QUARANTINE으로 보내고 배치의 나머지 레코드를 계속 처리합니다. DB 연결 실패, 입력 스키마 변경 등 처리 자체를 믿을 수 없는 오류는 DAG를 중단합니다. 품질 임계치 초과는 품질 결과와 판정을 먼저 보존한 후 마지막 태스크에서 DAG 실패로 표시합니다. Phase 8에서 실제 실패·재시도·복구·롤백을 확인했습니다.

소비자는 현재 처리 UUID에 연결된 게이트 PASS를 확인해야 합니다. CLEAN에 행이 있다는 사실만으로 배치 품질 승인을 의미하지 않습니다. 외부 시스템 배포·사용을 자동 차단하는 연동은 범위 밖입니다.

## 실행 환경과 트랜잭션

WSL 생성기가 프로젝트 data에 입력을 만들고 Airflow는 코드·SQL·입력을 읽기 전용으로 읽습니다. UI는 호스트 127.0.0.1:8080에만 게시하고 PostgreSQL은 Docker 내부 네트워크로 접속합니다. Airflow 메타데이터와 프로젝트 DB는 각각 별도 named volume입니다. DB 비밀파일은 OneDrive 밖 WSL 홈에 두고 관리자·실행 계정을 분리합니다.

RAW 적재, 결과 교체, 마트 구축, 게이트 기록은 각각의 트랜잭션입니다. 전체 여섯 태스크가 하나의 DB 트랜잭션은 아닙니다. 같은 배치 advisory lock으로 단계별 동시 쓰기를 직렬화합니다. 결과 교체는 이전 마트를 함께 무효화하고 집계·게이트가 현재 처리 UUID를 확인합니다. 결과/마트의 전체 과거 행은 누적하지 않으며 성공 처리 요약과 게이트 판정 이력은 유지합니다.

## 첫 버전에서 제외

실제 업무 데이터, LLM API 호출, 프롬프트 평가, dbt, Great Expectations, 외부 대시보드와 알림 연동은 첫 버전에 포함하지 않습니다.
