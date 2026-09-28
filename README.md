# Airflow Data Quality Pipeline

가상의 AI/OCR 상품 정보 추출 데이터를 Python, PostgreSQL, Apache Airflow로 수집·검증·분리 적재하고, 품질 지표와 배치 사용 기준을 관리하는 로컬 ETL 학습 프로젝트입니다.

이 프로젝트의 목표는 원본을 저장하는 것에서 출발해, 규칙별 오류를 추적하고 정상 데이터와 문제 데이터를 분리하며 장애 후 재실행 결과까지 설명하는 것입니다.

## 1. 프로젝트 개요

### 목적

AI/OCR 추출 결과에서 발생할 수 있는 NULL, 누락, 중복, 잘못된 값, 근거가 부족한 속성 후보를 가상 상품 데이터로 재현했습니다. 이를 데이터 엔지니어링 관점에서 검사·격리·집계하는 배치 파이프라인을 구현했습니다.

학습하는 핵심 내용:

```text
Python·SQL 기반 배치 ETL
PostgreSQL RAW / CLEAN / QUARANTINE / MART 계층
규칙 ID 기반 품질 검사와 다중 이슈 추적
Airflow TaskFlow DAG·재시도·실패 경계
배치·DAG run·처리 UUID 기반 재실행 관리
품질 임계치와 장애 복구 시연
빈 환경에서 기본 실행 재현
```

### 현재 구현 범위

Phase 0~9의 기본 버전과 후속 빈 환경 재현 확인을 완료했습니다.

```text
가상 데이터 생성
  -> 입력·manifest 검증
  -> PostgreSQL RAW 원본 적재
  -> 규칙 기반 품질 검사
  -> CLEAN / QUARANTINE / 이슈 저장
  -> 날짜·카테고리·모델별 품질 집계
  -> 배치 전체 격리율 게이트
```

실제 회사 데이터와 실제 LLM 호출은 사용하지 않습니다. `model_version`은 합성 데이터의 비교용 메타데이터입니다. `HALLUCINATION_CANDIDATE`는 제한된 규칙으로 표시하는 후보이며 환각 확정 판정이 아닙니다.

## 2. 아키텍처

전체 흐름:

```text
Ubuntu WSL: Synthetic Generator
  -> JSONL + Manifest
  -> Airflow DAG
       collect_input
       -> load_raw
       -> evaluate_quality
       -> load_results
       -> build_quality_metrics
       -> check_batch_thresholds
  -> PostgreSQL: RAW / CLEAN / QUARANTINE / MART
```

구성 요소별 역할:

| 구성 요소 | 역할 |
|---|---|
| Python 생성기 | 고정 시드로 정상·오류 상품 데이터 생성 |
| Airflow | 태스크 순서·재시도·실행 상태·로그 관리 |
| Python 품질 엔진 | 정규화·10개 규칙 검사·행별 판정 |
| PostgreSQL | 원본·최신 결과·품질 마트·처리/판정 이력 저장 |
| Docker Compose | 로컬 서비스·네트워크·볼륨·비밀파일 연결 |
| Ubuntu WSL | 개발 명령·환경 준비·입력 생성·검증 실행 |

Docker Compose 서비스:

| 서비스 | 이미지 | 주소·포트 | 설명 |
|---|---|---|---|
| Airflow | `apache/airflow:3.3.2` | `127.0.0.1:8080` | standalone / LocalExecutor |
| 프로젝트 DB | `postgres:16` | Docker 내부 `project-postgres:5432` | 호스트 포트 미게시 |

Airflow 자체 메타데이터와 프로젝트 DB는 별도 named volume을 사용합니다. 코드·SQL·입력은 Airflow에 읽기 전용으로 연결합니다. 입력 생성은 DAG 밖에서 수행하며 현재 스케줄은 수동, catchup 없음, 동시 run 1개입니다.

관련 문서: [아키텍처](docs/ARCHITECTURE.md), [데이터 모델](docs/DATA_MODEL.md), [결정 기록](docs/DECISIONS.md).

## 3. 실행 방법

### 3.1 환경 준비

모든 명령은 **Ubuntu WSL의 Bash**에서 실행합니다. Docker Desktop을 실행하고 Settings → Resources → WSL Integration에서 Ubuntu 연동을 활성화합니다. 호스트 Python은 3.10 이상 문법을 사용하며 확인한 버전은 3.14.4입니다. 별도 호스트 pip 설치는 필요하지 않습니다.

```bash
git clone https://github.com/dowon-jh/airflow-data-quality-pipeline.git
cd airflow-data-quality-pipeline
docker version
docker compose version
python3 --version
python3 scripts/setup_local.py
docker compose ps
```

setup의 역할:

```text
WSL 홈에 DB 비밀파일 준비
일반 설정 .env 생성
PostgreSQL 기동
schema·table·실행 계정 권한 적용
Airflow 기동
```

새 환경에서는 setup을 먼저 실행합니다. DB 비밀파일은 `~/.config/ai-output-dq-pipeline/secrets/`에 두며 암호 값은 `.env`에 넣지 않습니다.

### 3.2 Airflow 로그인·DAG 확인

접속 주소는 `http://localhost:8080`, 사용자 이름은 `admin`입니다. 비밀번호는 로컬에서 확인합니다.

```bash
docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
docker compose exec -T airflow airflow dags list
docker compose exec -T airflow airflow dags list-import-errors -o json
```

기동 직후에는 DAG 등록까지 시간이 필요합니다. `ai_output_quality_pipeline`이 목록에 있고 import 오류가 없는 것을 확인합니다.

### 3.3 입력 생성·DAG 실행

```bash
python3 src/generator/generate_product_data.py
cat data/generated/products.manifest.json
bash scripts/trigger_pipeline.sh
docker compose exec -T airflow airflow dags list-runs ai_output_quality_pipeline -o json
```

trigger 성공은 실행 접수입니다. UI의 해당 run에서 여섯 태스크가 완료됐는지 확인합니다. 실행 중 코드·입력·manifest를 변경하지 않습니다.

### 3.4 결과 확인·종료

```bash
docker compose exec -T project-postgres psql -X -U etl_user -d etl_project \
  < sql/etl/007_inspect_results.sql
docker compose exec -T project-postgres psql -X -U etl_user -d etl_project \
  < sql/mart/009_inspect_quality_mart.sql
docker compose down
```

기본 입력은 RAW 1,000·CLEAN 959·격리 41·이슈 44·마트 5그룹·4.1% PASS입니다. `down`은 named volume을 보존합니다. 준비된 환경의 재시작은 `docker compose up -d`입니다.

상세 설치·배치별 조회·복구: [실행 안내서](docs/RUNBOOK.md), [로컬 보안 설정](docs/LOCAL_SECURITY_SETUP.md).

## 4. 입력 데이터

| 항목 | 내용 |
|---|---|
| 기본 데이터 | 1,000행, seed 42, 가상 모델 v1.0 |
| 오류 주입 | 전체 4%, 그 안에 속성 NULL 1% 포함 |
| 입력 | `data/generated/products.jsonl` |
| manifest | 배치 ID·옵션·행 수·SHA-256 |
| 라벨 | 별도 `products.labels.jsonl`; RAW/DAG 입력에서 제외 |
| 예시 | `data/sample/`의 30행; 오류 유형 관찰용 |

주요 필드는 `source_record_id`, `batch_id`, `product_id`, `category`, `original_text`, `ocr_text`, `extracted_attribute`, `attribute_type`, `model_version`, `created_at`입니다.

같은 생성기 버전과 옵션은 같은 기본 입력과 배치를 만듭니다. 같은 배치에 같은 내용은 RAW를 늘리지 않고, 같은 배치에 다른 내용은 차단합니다. 입력 수정은 새 배치로 적재합니다.

관련 문서: [가상 데이터](docs/SYNTHETIC_DATA.md), [RAW와 데이터 모델](docs/DATA_MODEL.md).

## 5. 품질 검사와 정제 방식

품질 엔진: `src/dq/engine.py`, 규칙 버전 1.1.

| 규칙 | 검사 내용 | 처리 |
|---|---|---|
| DQ001 | 상품 ID NULL·공백 | ERROR → 격리 |
| DQ002 | 상품 ID 형식 | ERROR → 격리 |
| DQ003 | 정규화 ID의 배치 내 중복 | 그룹 전체 격리 |
| DQ004 | 허용 카테고리 | ERROR → 격리 |
| DQ005 | 추출 속성 NULL | ERROR → 격리 |
| DQ006 | 추출 속성 빈 문자열·공백 | ERROR → 격리 |
| DQ007 | 속성에 글자·숫자가 없음 | ERROR → 격리 |
| DQ008 | 정규화 속성이 200자 초과 | ERROR → 격리 |
| DQ009 | OCR과 원문 불일치 | WARNING; 단독이면 CLEAN |
| DQ010 | 같은 숫자·단위 원문 근거가 없는 후보 | ERROR → 격리; 의미 전체 검증은 아님 |

Unicode NFKC·공백 정리·ID/카테고리 대문자 정규화를 적용합니다. ERROR가 하나 이상이면 격리하며 한 행의 모든 이슈를 기록합니다. RAW는 원본 그대로, CLEAN은 정규화 값, QUARANTINE은 원본 업무 값을 유지합니다.

경고도 이슈 테이블에 보존합니다. 개별 JSON 보고서는 `bash scripts/evaluate_quality.sh`로 생성하며 DAG가 이 파일을 갱신하지는 않습니다.

관련 문서: [규칙 정의](docs/DATA_QUALITY_RULES.md), [품질 엔진](docs/PHASE4_QUALITY_ENGINE.md), [분리 적재](docs/PHASE5_SPLIT_LOAD.md).

## 6. PostgreSQL 데이터 계층

| 테이블·뷰 | 역할 |
|---|---|
| `raw.ai_product_output` | 오류값까지 포함한 입력 원본 |
| `raw.ingest_batch` | 배치 해시·행 수·적재 추적 |
| `clean.ai_product_output` | 품질 검사 통과 행 |
| `quarantine.ai_product_output` | ERROR가 있는 행 |
| `quarantine.dq_issue` | 규칙 ID·필드·심각도·설명·처리 UUID |
| `clean.processing_run` | 성공 처리 요약·DAG run·규칙 버전·최신 여부 |
| `mart.daily_quality_metrics` | 배치·UTC 날짜·카테고리·모델별 지표 |
| `mart.daily_rule_metrics` | 그룹별 규칙·심각도 이슈 수 |
| `mart.batch_quality_gate` | 실행·정책별 PASS/FAIL 판정 이력 |
| `mart.current_daily_quality_metrics` | 현재 처리 결과의 마트 조회 뷰 |

RAW는 보존하고 최신 결과·이슈·마트는 배치별 교체합니다. 성공 처리 요약과 게이트 판정은 누적하지만 과거 결과 행 전체를 저장하지는 않습니다. 결과 교체와 이전 마트 무효화는 한 트랜잭션입니다. 전체 DAG가 하나의 트랜잭션인 것은 아닙니다.

같은 현재 처리 UUID 재시도는 이력을 늘리지 않습니다. 새 run은 새 처리 UUID·이력을 만들며 더 새 실행으로 대체된 이전 run 재사용은 거부합니다. 배치 advisory lock으로 동시 쓰기를 직렬화하고 XCom에는 해시·ID·집계만 전달합니다.

## 7. 품질 게이트와 실패 시나리오

정책 1.0은 **배치 전체 격리율 > 5%이면 FAIL**입니다. 정확히 5%는 PASS입니다. 5%는 학습용 기준이며 실무 SLA가 아닙니다.

| 시나리오 | 기대 동작 |
|---|---|
| 기본 배치 4.1% | 전체 DAG 성공 |
| 개별 행 오류 | 해당 행 격리, 나머지 처리 계속 |
| 정확히 5% | PASS |
| 6%·가상 모델 v1.1 NULL 20% | 조사 결과 보존 후 최종 게이트 실패 |
| 스키마 누락·빈 입력·manifest 불일치 | 해당 단계 실패·후속 차단 |
| 같은 배치에 다른 내용 | RAW 충돌 차단 |
| DB 일시 장애 | 자동 재시도 최대 2회, 총 3시도 |
| DB 장애 지속 | 재시도 소진 후 차단, DB 복구 후 특정 run 재실행 |

게이트 FAIL이어도 CLEAN·QUARANTINE·마트와 판정은 남습니다. 소비자는 최신 처리 UUID에 대응하는 PASS를 확인해야 합니다. 그룹별/NULL 비율 차단과 외부 배포 자동 차단은 구현하지 않았습니다.

관련 문서: [Airflow DAG](docs/PHASE6_AIRFLOW_DAG.md), [품질 마트·게이트](docs/PHASE7_QUALITY_MART.md), [장애 시연](docs/PHASE8_FAILURE_RECOVERY.md).

## 8. 실제 실행 결과

### 8.1 기본 배치

| 항목 | 결과 |
|---|---:|
| 입력 / CLEAN / QUARANTINE | 1,000 / 959 / 41 |
| WARNING 행 / 전체 이슈 | 3 / 44 |
| 마트 그룹 | 5 |
| NULL 속성 / 중복 ID 행 / 근거 후보 행 | 10 / 8 / 3 |
| 격리율 / 게이트 | 4.1% / PASS |

오류 주입 40행에 중복의 원래 행 4개가 추가되고 경고만 있는 3행은 통과하므로 `40 + 4 - 3 = 41`입니다. 이슈 수와 격리 행 수는 구분합니다.

### 8.2 품질 마트

UTC 2026-09-01, 가상 모델 v1.0의 기본 배치:

| 카테고리 | 입력 | CLEAN | 격리 | 경고 | NULL 속성 |
|---|---:|---:|---:|---:|---:|
| BAG | 245 | 237 | 8 | 0 | 5 |
| LAMP | 264 | 253 | 11 | 0 | 0 |
| SHOES | 246 | 238 | 8 | 2 | 3 |
| TUMBLER | 242 | 231 | 11 | 1 | 2 |
| UNKNOWN | 3 | 0 | 3 | 0 | 0 |

집계 날짜는 원천 UTC 날짜이며 결측이면 적재일 UTC로 대체하고 누락 건수를 기록합니다. NULL·빈 문자열 차원은 구분합니다.

### 8.3 장애·빈 환경 검증 요약

| 검증 | 실제 결과 |
|---|---|
| Phase 8 확정 13 DAG run | 7 success·6개 의도한 failed, 러너 PASS |
| DB 일시 중단 | 1차 실패 → 2차 성공 |
| DB 지속 중단 | 3회 실패 → 복구·특정 run clear → 4차 성공 |
| 결과/마트 commit 전 오류 | 이전 전체 행 스냅샷 복원 |
| 같은 UUID의 두 실제 DB 연결 | processed 1·already_processed 1 |
| setup commit 전/후 실패 | 이전 상태 복원 / forward recovery |
| 빈 환경 기본 실행 | 9개 테이블 0행에서 여섯 태스크 성공·기본 결과 재현 |

공개 증거 요약: [실행 결과 기록](docs/evidence/verification-summary.json). 상세 실험 조건과 미검증 범위는 아래 문서에 있습니다. 화면 캡처 대신 실제 조회값·run ID·상태를 기록했습니다.

## 9. 검증과 보안 개선

### 9.1 장애 실험 재현

아래 명령은 실험 입력을 추가하고 **실제 프로젝트 DB를 잠시 중지**합니다. 다른 DAG 실행이 없을 때 수행합니다.

```bash
python3 -u scripts/run_phase8.py
# 이번 러너가 출력한 evidence 경로로 교체
EVIDENCE='data/generated/phase8/이번_suite_id/evidence.json'
python3 scripts/check_setup_recovery.py "$(dirname "$EVIDENCE")/setup-recovery.json"
python3 scripts/check_phase8_final.py "$EVIDENCE"
```

### 9.2 빈 환경 재현

기존 기본 환경이 실행 중이고 8081 포트가 비어 있을 때:

```bash
python3 -u scripts/check_clean_install.py
```

고유 Compose 이름·새 비밀파일·빈 볼륨·별도 폴더에서 기본 실행을 확인합니다. 성공 후 검증용 서비스만 중지하고 결과는 보존합니다. 기존 DB 9개 테이블·설정·비밀파일·컨테이너 상태의 전후 불변도 확인했습니다. 같은 PC의 WSL·Docker·Python과 이미지 캐시를 재사용했으며 새 PC/OS 검증은 아닙니다.

### 9.3 보안 구성

| 항목 | 적용 |
|---|---|
| DB 계정 | 관리자 etl_user / 실행 etl_app 분리 |
| RAW 실행 권한 | SELECT·INSERT; UPDATE·DELETE 불허 |
| DDL | 관리자 setup에서만 적용 |
| 비밀파일 | WSL 홈에 저장, 디렉터리 0700·파일 0444, 읽기 전용 mount |
| 네트워크 | UI loopback, DB 호스트 포트 미게시 |
| 입력·코드 | 읽기 전용 mount; 데이터 상대 경로 검증 |
| 로그·XCom | 비밀 값·대량 원본 행 미기록 |

로컬 standalone 학습 환경입니다. 운영 인증·TLS·대용량·timeout 소진·crash recovery·백업 복원·이미지 CVE/침투 검증은 별도 과제입니다.

## 10. 트러블슈팅

### 10.1 `service airflow is not running`

원인: 컨테이너가 실행 중이 아닙니다. 처음이면 setup, 준비된 환경이면 시작 후 상태를 확인합니다.

```bash
docker compose up -d
docker compose ps
docker compose logs --tail=100 airflow
```

### 10.2 DAG가 목록에 없는 경우

기동 직후 등록 지연 또는 import 오류를 확인합니다.

```bash
docker compose exec -T airflow airflow dags list-import-errors -o json
docker compose logs --tail=100 airflow
```

### 10.3 같은 배치의 내용을 수정한 경우

같은 batch_id에 다른 SHA-256은 RAW 원본 보호를 위해 차단합니다. 입력 수정은 새 batch_id와 일치하는 manifest로 처리하고 이전 RAW는 유지합니다.

### 10.4 저장 결과가 있는데 DAG가 failed인 경우

최종 게이트는 품질 결과와 FAIL 판정을 커밋한 다음 실패를 표시합니다. 첫 실패 태스크와 `mart.batch_quality_gate`를 확인합니다. 동일한 나쁜 입력을 반복해도 PASS가 되지는 않습니다.

### 10.5 재실행과 clear

DB를 복구하고 UI에서 정확한 run·실패 태스크·후속 태스크를 선택해 clear합니다. 다른 run이 포함되지 않는지 확인합니다. 이미 새 처리로 대체된 run은 clear 대신 새 run을 사용합니다. Phase 8 예비 날짜 필터 CLI clear는 실제 attempt를 늘리지 않아 확정 러너를 명시적 run/task REST clear로 변경했습니다.

### 10.6 전체 RAW 건수가 1,000보다 큰 경우

기본 실행만 한 새 환경은 1,000행입니다. 개발 환경의 3,300행은 초기 배치 1,000·기본 배치 1,000·예비 실험 600·확정 실험 700의 합계입니다. 단순 재실행 중복 때문이 아닙니다. 조회할 때 batch_id를 구분합니다.

### 10.7 기존 DB 볼륨은 있는데 비밀파일이 없는 경우

setup은 원래 암호 없이 새 암호를 임의 생성하지 않습니다. 원래 비밀파일을 복구합니다. DB 볼륨과 비밀파일은 별개이므로 프로젝트 폴더 복사만으로 과거 DB 환경이 복제되지는 않습니다.

## 11. 향후 확장 방향

기본 버전 완료 후 선택할 과제:

```text
규칙별 경계·회귀 자동 테스트
대용량 성능·백업 복원·운영 보안 검증
정기 입력 공급·스케줄·백필
업무 근거 기반 임계치·모델/카테고리별 게이트
품질 대시보드·외부 알림
LLM API·프롬프트/모델 평가
dbt·Great Expectations 연동
```

현재 구현·완료 기준에는 포함하지 않습니다.

## 12. 참고 문서

| 분류 | 문서 |
|---|---|
| 기획·개발 순서 | [프로젝트 기획안](AIRFLOW_PROJECT_BRIEF.md), [로드맵](ROADMAP.md) |
| 실행·복구 | [RUNBOOK](docs/RUNBOOK.md), [보안 설정](docs/LOCAL_SECURITY_SETUP.md) |
| 통합 기록 | [Phase 0~9 요약](docs/PROJECT_SUMMARY.md), [진행 기록](docs/PROGRESS.md) |
| 설계 | [아키텍처](docs/ARCHITECTURE.md), [데이터 모델](docs/DATA_MODEL.md), [결정 기록](docs/DECISIONS.md) |
| 입력·검사 | [가상 데이터](docs/SYNTHETIC_DATA.md), [규칙](docs/DATA_QUALITY_RULES.md), [Phase 4](docs/PHASE4_QUALITY_ENGINE.md) |
| 적재·DAG·마트 | [Phase 5](docs/PHASE5_SPLIT_LOAD.md), [Phase 6](docs/PHASE6_AIRFLOW_DAG.md), [Phase 7](docs/PHASE7_QUALITY_MART.md) |
| 검증 실험 | [Phase 8](docs/PHASE8_FAILURE_RECOVERY.md), [빈 환경 재현](docs/CLEAN_INSTALL_VERIFICATION.md) |
| 최종 완료 기준 | [Phase 9 보고](docs/PHASE9_FINAL_REPORT.md) |
| 기능·보안 검토 | [초기 검토](docs/SECURITY_REVIEW.md), [재검토](docs/PHASE4_REVIEW.md), [개선 결과](docs/PHASE4_REMEDIATION.md) |

문서는 [Security Log ETL](https://github.com/dowon-jh/Security-Log-ETL)의 번호형 구성·역할 표·명령 설명·실험 기록·트러블슈팅 양식에 맞춰 정리했습니다. 기존 단계 문서의 건수와 미구현 설명은 당시 기록이며 현재 상태는 이 README와 최종 보고를 기준으로 읽습니다.
