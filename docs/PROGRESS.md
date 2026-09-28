# 프로젝트 진행 기록

기준일: 2026-09-28

## 현재 상태

Phase 0~9 기본 버전을 완료했습니다. 실제 DAG의 품질 저하·입력 오류·재처리·DB 중단/복구와 결과/마트 롤백·동시 처리·준비 작업 복구를 검증하고 최종 실행·복구 문서를 정리했습니다. 현재 RAW 전체 3,300행과 기본 결과 959/41/44는 기존 실행 기록이며 새 설치의 기본 실행은 RAW 1,000행입니다. 서비스는 정상 실행 중입니다. 실행 절차: [RUNBOOK](RUNBOOK.md), 완료 범위: [최종 보고](PHASE9_FINAL_REPORT.md). 아래 수치·미수행 설명은 각 단계 당시 이력입니다.

## 완료 내용

### Phase 0 — 설계

다음 문서를 작성해 목표, 범위, 데이터 흐름, 계층, 품질 규칙과 장애 처리 기준을 정리했습니다.

- 프로젝트 개요: `README.md`, `AIRFLOW_PROJECT_BRIEF.md`
- 아키텍처: `docs/ARCHITECTURE.md`
- 데이터 모델: `docs/DATA_MODEL.md`
- 품질 규칙: `docs/DATA_QUALITY_RULES.md`
- 장애 시나리오: `docs/FAILURE_SCENARIOS.md`
- 의사결정 기록: `docs/DECISIONS.md`
- 개발 순서: `ROADMAP.md`

첫 버전은 Python·PostgreSQL·Airflow·Docker Compose와 규칙 기반 DQ 검사로 범위를 정했습니다. 외부 LLM 호출과 프롬프트 평가는 이후 확장 후보입니다.

### Phase 1 — 로컬 환경

- Ubuntu WSL 2에서 개발 명령을 실행하도록 결정했습니다.
- Docker Desktop의 Ubuntu WSL Integration을 활성화했습니다.
- Airflow 3.3.2 `standalone` / `LocalExecutor` 컨테이너를 실행합니다.
- 프로젝트 데이터용 PostgreSQL 16 컨테이너를 별도로 실행합니다.
- Airflow 메타데이터는 Airflow 컨테이너의 볼륨에, 프로젝트 DB 데이터는 PostgreSQL 볼륨에 저장합니다.
- 초기에는 `.env`에 DB 암호를 저장했습니다. 2026-09-28에 WSL 홈의 비밀파일과 Compose secrets로 전환하고 암호를 교체했습니다.

현재 서비스 상태 확인 결과:

| 서비스 | 이미지 | 상태 | 로컬 주소 |
|---|---|---|---|
| Airflow | `apache/airflow:3.3.2` | 실행 중 | `http://localhost:8080` |
| 프로젝트 PostgreSQL | `postgres:16` | healthy | Docker 내부 `project-postgres:5432` |

Airflow 상태 API 응답은 HTTP 200이며 metadatabase, scheduler, triggerer, DAG processor가 healthy였습니다. Airflow 컨테이너에서 프로젝트 PostgreSQL로 `SELECT 1` 접속도 확인했습니다.

### Phase 2 — 가상 데이터

- 표준 라이브러리만 사용하는 Python 생성기를 `src/generator/generate_product_data.py`에 작성했습니다.
- `source_record_id`, `batch_id`, 상품·OCR·추출 필드, `model_version`, UTC `created_at`을 JSONL로 생성합니다.
- 기본 설정은 1,000행, seed 42, 전체 오류 주입률 4%, 속성 NULL 주입률 1%입니다.
- 기본 실행 결과는 960개 오류 미주입 행과 40개 주입 오류 행입니다.
- 주입 오류 유형은 DQ001-DQ010에 대응하는 10개입니다.
- 정답 라벨은 입력 데이터와 분리된 `*.labels.jsonl`에, 생성 옵션·오류 분포·SHA-256은 `*.manifest.json`에 기록합니다.
- 같은 설정으로 재생성한 JSONL 파일이 `cmp` 비교에서 동일한 것을 확인했습니다.

저장된 산출물:

| 파일 | 내용 |
|---|---|
| `data/sample/products.jsonl` | 모든 오류 유형을 살펴보기 위한 30행 예시 |
| `data/sample/products.labels.jsonl` | 예시 15개 주입 오류 라벨 |
| `data/sample/products.manifest.json` | 예시 생성 설정과 분포 |
| `data/generated/products.jsonl` | 기본 1,000행 배치 |
| `data/generated/products.labels.jsonl` | 기본 배치의 40개 주입 오류 라벨 |
| `data/generated/products.manifest.json` | 기본 배치 설정과 분포 |

`data/generated/`는 Git에서 무시합니다. 30행 샘플은 오류 유형 확인용으로 오류율이 50%이므로 정상 배치의 비율 기준으로 쓰지 않습니다. 라벨 파일은 RAW 입력에 적재하지 않습니다. 중복 ID 그룹은 원래 행까지 DQ003 대상이 될 수 있어, `normal_count`와 실제 CLEAN 건수는 다를 수 있습니다.

### Phase 3 — PostgreSQL RAW 적재

- `sql/schemas/001_create_layers.sql`로 `raw`, `clean`, `quarantine`, `mart` schema를 생성했습니다.
- `sql/raw/002_create_raw_tables.sql`로 `raw.ai_product_output`과 배치 적재 이력 테이블 `raw.ingest_batch`를 만들었습니다.
- `src/load/load_raw.py`가 JSONL 필드, 단일 배치 ID, 고유 원천 행 ID, 매니페스트 SHA-256을 확인하고 한 트랜잭션으로 적재합니다.
- Airflow 컨테이너에 `src`, `sql`, `data` 디렉터리를 읽기 전용으로 연결해 WSL의 `docker compose exec` 명령으로 실행했습니다.
- 기본 배치 `batch-208ca532eb1f`를 적재했습니다.

적재 후 읽기 전용 SQL 조회 결과:

| 항목 | 건수 |
|---|---:|
| RAW 총 행 | 1,000 |
| NULL `product_id` | 4 |
| NULL `extracted_attribute` | 10 |
| 공백 `extracted_attribute` | 3 |
| 중복 `product_id` 그룹 | 4 |

`ingested_at`은 DB에서 부여하며, 업무 필드에는 품질 제약을 걸지 않아 잘못된 값도 RAW에 보존했습니다. 주입 라벨 파일은 적재 대상에서 제외했습니다.

## 2026-09-28 보안 개선

- 관리자 `etl_user`와 실행 계정 `etl_app`을 분리했습니다.
- RAW 실행 권한은 SELECT·INSERT이며 DDL은 `scripts/setup_local.py`의 관리자 작업으로 분리했습니다.
- 관리자 암호를 교체하고 암호 파일을 `/home/dwjam/.config/ai-output-dq-pipeline/secrets/`로 옮겼습니다.
- `.env`와 컨테이너 설정 환경변수에서 암호·연결 URI를 제거했습니다.
- PostgreSQL 호스트 포트 5433을 제거하고 기존 볼륨은 유지했습니다.
- 자세한 적용 방법: [로컬 보안 설정](LOCAL_SECURITY_SETUP.md).

실행 결과: `etl_app`의 관리자 관련 권한은 모두 false이고 RAW SELECT·INSERT는 true, UPDATE·DELETE 및 schema/DB 생성 권한은 false였습니다. 기존 배치의 SHA-256과 1,000행이 유지됐습니다. 제한 계정으로 RAW 적재기를 실행해 `already loaded: 1000 rows`로 종료했습니다. 암호 파일 줄바꿈 처리 오류를 수정한 뒤 접속이 정상화됐습니다.

## Phase 4 — 품질 검사 엔진 (2026-09-28)

- DQ001-DQ010을 `src/dq/engine.py`에 구현했습니다. 한 행에 발생한 모든 이슈를 기록합니다.
- 중복 그룹 전체는 격리 대상, DQ009 OCR 불일치는 WARNING으로 확정했습니다. WARNING만 있으면 PASS입니다.
- `src/dq/evaluate_raw.py`는 제한 계정으로 RAW를 읽어 검사하며, WSL의 `scripts/evaluate_quality.sh`가 JSON 보고서를 저장합니다.
- 길이 초과 예시가 실제 180자인 생성기 오류를 발견해 240자로 수정하고 생성기 버전 1.1을 추가했습니다.
- 새 배치 `batch-a2ebfbe5e814`: 입력 1,000행, PASS 대상 959행, 격리 대상 41행, 경고 3행, 이슈 44개입니다. 경고 3행은 PASS에 포함됩니다.
- 당시 RAW 전체는 두 배치 2,000행이었고 CLEAN·QUARANTINE 적재와 DAG 자동화는 미구현 상태였습니다. 이후 Phase 5에서 분리 적재를 구현했습니다.
- 상세 기록: [Phase 4 품질 엔진](PHASE4_QUALITY_ENGINE.md).

```bash
bash scripts/evaluate_quality.sh
```

## Phase 5 — 분리 적재 (2026-09-28)

- 결과·이슈·성공 처리 이력 테이블 4개를 생성했습니다. 실행 계정으로 DDL 없이 처리합니다.
- 기본 배치 `batch-a2ebfbe5e814`를 RAW에서 다시 평가해 CLEAN 959행·QUARANTINE 41행·이슈 44개를 저장했습니다.
- CLEAN 행에 연결된 WARNING 3개, 양쪽 결과 중복 0개, QUARANTINE 업무 값·원천 적재 시각의 RAW 대비 차이 0개를 조회했습니다.
- 첫 실행·같은 UUID 재시도·새 UUID 재처리를 수행했습니다. 같은 UUID는 `already_processed`로 반환했고 새 실행 후 결과 건수는 유지됐습니다. 성공 요약 이력은 2개, 최신 표시는 1개입니다.
- RAW는 총 2,000행이며 초기 배치는 이번에 분리 적재하지 않았습니다.
- 최신 결과는 배치별 트랜잭션 교체, 성공 실행 요약은 누적으로 확정했습니다. 빈 배치는 오류로 중단합니다.
- 상세 기록: [Phase 5](PHASE5_SPLIT_LOAD.md). 실제 동시 실행·실패 롤백 시연은 후속 검증입니다.

## 일상적인 WSL 명령

프로젝트 루트에서 아래 명령으로 실행합니다.

```bash
cd "/mnt/c/Users/dwjam/OneDrive/문서/ChatGPT/arirflowETL프로젝트"
docker compose ps
python3 src/generator/generate_product_data.py
docker compose exec -T airflow python /opt/airflow/project/src/load/load_raw.py \
  --input /opt/airflow/project/data/generated/products.jsonl
docker compose exec -T project-postgres psql -U etl_user -d etl_project \
  < sql/raw/003_inspect_raw.sql
```

컨테이너가 멈췄으면 다음 명령으로 다시 시작합니다.

```bash
docker compose up -d
```

Airflow 계정은 사용자 `admin`이며, 비밀번호는 WSL에서 아래 명령으로 확인합니다. 값은 이 문서에 저장하지 않습니다.

```bash
docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

## Phase 4 이후 검토 이력

**후속 개선 적용:** 검토 이후 숫자 파서를 규칙 1.1로 갱신했고 준비 스크립트의 실패 경계·복구 안내를 개선했습니다. 재처리 정책을 정리하고 기본 DB 접근을 제한했습니다. 기본 배치 결과와 RAW 2,000행이 유지됐습니다. [Phase 4 개선 기록](PHASE4_REMEDIATION.md)을 참고하세요. 아래 검토 문장은 개선 전 이력입니다.

2026-09-28 Phase 4 이후 재검토: 숫자·단위 파서의 쉼표 부분 일치 오판, 환경 준비 실패 후 상태 복구 누락, 같은 배치 수정 문서와 RAW 불변 정책의 충돌, 프로젝트 외 기본 DB 접속 권한을 확인했습니다. 실제 코드·권한 수정은 아직 적용하지 않았습니다. [검토 결과와 재현 방법](PHASE4_REVIEW.md)을 참고하세요. Phase 5 전에 숫자 파서와 준비 작업 실패 복구를 우선 개선하는 것을 권장합니다.

이후 Phase 5~9 구현·검증·최종 문서를 완료했습니다. 현재 실행에는 아래 과거 개별 명령보다 [RUNBOOK](RUNBOOK.md)의 전체 DAG 절차를 사용합니다.

## Phase 6 — Airflow DAG (2026-09-28)

- TaskFlow DAG `ai_output_quality_pipeline`을 추가했습니다. 태스크는 `collect_input → load_raw → evaluate_quality → load_results`입니다.
- 수동 실행, catchup 없음, 동시 run 1개, 인프라 재시도 2회와 timeout을 설정했습니다. 입력·해시·메타데이터 오류는 재시도 없이 실패합니다.
- XCom에는 상대 경로·배치·해시·규칙 버전·집계만 저장하고 행 데이터와 암호는 전달하지 않습니다.
- run ID 기반 UUID5를 결과 처리 ID로 사용합니다. 결과 적재 전에 선행 품질 요약과 RAW 재평가 값을 비교합니다.
- 실제 run `manual__2026-09-28T06:58:12.656512+00:00`: 네 태스크 모두 `success`.
- DB 결과: CLEAN 959행, QUARANTINE 41행, 이슈 44개, CLEAN의 WARNING 3개. RAW 총 2,000행, 성공 이력 3개·최신 1개.
- 기존 RAW 배치를 재사용해 정상 실행했습니다. 신규 배치 최초 DAG 적재·실패·자동 재시도·task clear·동시 실행 실험은 미수행입니다.
- 예외 import를 SDK 경로로 갱신했습니다. 상세 실행 증거·WSL 명령: [Phase 6](PHASE6_AIRFLOW_DAG.md).

## Phase 7 — 품질 마트·게이트 (2026-09-28)

- 날짜·카테고리·모델·배치별 최신 마트, 규칙 상세, 실행별 게이트 이력과 현재 조회 뷰를 생성했습니다.
- 원천 생성 UTC 날짜를 집계일로 사용하고 결측이면 원천 적재일로 대체합니다. NULL·빈 문자열·잘못된 차원도 보존합니다.
- 새 분리 적재와 동시에 이전 마트를 삭제합니다. 집계·게이트는 현재 처리 UUID와 건수를 확인하고 같은 배치 lock을 사용합니다.
- 정책 1.0: 배치 전체 격리율 > 5%이면 실패. 판정 커밋 뒤 DAG 실패를 표시합니다. 그룹별/NULL 게이트는 포함하지 않습니다.
- 실제 run `manual__2026-09-28T07:16:11.179345+00:00`의 여섯 태스크 모두 성공. 처리 UUID `8157c9af-613e-5d28-933f-a5a309332ba7`.
- 마트 5그룹, 입력 1,000·통과 959·격리 41·이슈 44·경고 3·NULL 속성 10·중복 ID 8·근거 후보 3. 배치 격리율 4.1%, 게이트 PASS.
- 기존 RAW 2,000행 유지. 성공 분리 적재 이력은 이전 3개와 이번 1개로 총 4개, 기본 배치 최신 표시 1개입니다. 게이트 이력은 이번 1개이며 과거 run에 소급 적용하지 않았습니다.
- 임계치 초과·경계·자동 재시도·롤백 실험은 아직 수행하지 않았습니다. [Phase 7 구현·실행 기록](PHASE7_QUALITY_MART.md).

## Phase 8 — 장애·재실행·복구 (2026-09-28)

- 확정 묶음 `20260928T075054`, 16:50~16:56 KST, 13개 실제 DAG run·증거 이벤트 32개, 러너 PASS.
- 정상 신규 RAW·한 행 격리·정확히 5% PASS·6%/20% FAIL과 결과/마트/판정 보존을 확인했습니다.
- NULL/빈 차원 분리·생성 시각 결측 2행 적재일 대체·UTC 시간대 변환을 확인했습니다.
- 필수 컬럼 누락·빈 입력·같은 배치 다른 내용은 단계별로 차단됐고 부적절한 RAW는 적재되지 않았습니다.
- 같은 run clear는 동일 UUID·이력 증가 없음, 새 run은 결과 건수 유지·새 이력 1개, superseded run clear는 거부했습니다. REST API preview로 정확한 run/태스크만 선택하고 실제 attempt 증가도 확인했습니다.
- 실제 분리 적재/마트 교체 후 commit 전 오류를 주입해 전체 이전 행 스냅샷이 복원됐습니다. 같은 UUID 두 연결은 processed 1개·already_processed 1개로 처리됐습니다.
- DB 일시 중단: load_raw 1차 실패 후 2차 성공. 지속 중단: 3회 실패 후 후속 차단, 복구·특정 run clear 후 4차 성공.
- 환경 준비 commit 전 실패와 commit 후 설정 publication 실패를 주입하고 이전 상태 복원/forward recovery를 확인했습니다. 비밀파일·env 내용은 그대로입니다.
- 최초 예비 러너의 CLI clear가 실제 재실행을 만들지 않아 중단·수정했습니다. 예비 6배치 600행과 확정 7배치 700행을 보존합니다. 기존 2배치 전체 내용은 hash 비교에서 동일하며 RAW 전체 3,300행입니다.
- 세부 근거·WSL 명령·범위: [Phase 8](PHASE8_FAILURE_RECOVERY.md). 실제 timeout 소진·부하/crash recovery·CVE/침투 검증은 범위 밖입니다.

## Phase 9 — 최종 문서·재현 절차 (2026-09-28)

- README에 올바른 첫 준비 순서(setup 먼저)와 전체 DAG 빠른 실행을 정리했습니다.
- RUNBOOK에 로그인·DAG 등록·입력 생성·실행 상태·기본 배치 조회·기대 결과·실패별 복구·시연 재현·종료·증거 보관을 작성했습니다.
- 최종 보고에 기획안의 8개 완료 기준, 실제 증거, 운영 의미와 미검증 범위를 대응시켰습니다.
- 아키텍처·결정 기록·통합 정리·로드맵을 현재 여섯 태스크와 기본 버전 완료 상태에 맞췄습니다.
- 현재 서비스와 DAG import 상태는 읽기 전용으로 확인했습니다. 새 DAG 실행·장애 실험·새 PC 초기 설치는 Phase 9에서 실행하지 않았습니다.
- 실무·LLM 확장은 기본 버전 완료 후 선택 과제입니다.

## 후속 — 빈 환경 재현 확인 (2026-09-28)

- 별도 폴더·고유 Compose 이름·8081 포트·새 비밀파일·새 DB/Airflow 볼륨을 준비했습니다. setup 후 데이터 테이블 9개 모두 0행이었습니다.
- 기본 입력 생성과 DAG 최초 실행: 여섯 태스크 success, RAW 1,000·CLEAN 959·격리 41·이슈 44·마트 5·4.1% PASS.
- 기존 DB 9개 테이블 전체 행·설정·비밀파일·컨테이너 ID와 상태는 전후 동일했습니다.
- 검증용 서비스는 중지하고 결과를 보존했습니다. 같은 PC의 Docker/WSL/Python·이미지 캐시를 사용했으며 별도 PC/새 OS 검증은 아닙니다.
- 실행 묶음 `20260928T091708-656fae`, [확인 보고](CLEAN_INSTALL_VERIFICATION.md), 러너 `scripts/check_clean_install.py`.
