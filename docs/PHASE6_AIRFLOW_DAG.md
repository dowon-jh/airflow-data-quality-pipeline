# Phase 6 — Airflow DAG 연결

기준일: 2026-09-28. DAG ID: `ai_output_quality_pipeline`.

후속 상태: 이 문서는 Phase 6의 네 태스크 연결 기록입니다. 현재 DAG는 Phase 7 집계·게이트를 추가해 여섯 태스크이며 최신 구현·실행은 [Phase 7](PHASE7_QUALITY_MART.md)을 참고하세요.

## 무엇을 연결했나

```text
WSL 생성기로 JSONL + manifest 준비
    → collect_input
    → load_raw
    → evaluate_quality
    → load_results
```

| 태스크 | 역할 | 다음 태스크에 전달하는 값 |
|---|---|---|
| `collect_input` | 프로젝트 data 안의 입력 경로, JSONL 구조·배치·행 수·SHA-256·manifest 확인 | 상대 경로, 배치 ID, 행 수, 해시 |
| `load_raw` | 수집한 입력의 동일성을 다시 확인하고 RAW에 멱등 적재 | 동일한 작은 입력 메타데이터 |
| `evaluate_quality` | 커밋된 RAW에 규칙 10개를 적용 | 배치·규칙 버전·입력 해시·건수 및 규칙별 집계 |
| `load_results` | 품질 판정 동일성을 확인한 뒤 CLEAN·QUARANTINE·이슈·성공 이력을 원자적으로 저장 | 처리 UUID·상태·작은 결과 요약 |

생성기는 WSL에서 별도로 실행합니다. DAG의 수집 단계는 준비된 로컬 파일을 읽습니다. 현재 read-only data mount를 유지하며 입력 원본이나 라벨을 쓰지 않습니다. 수집기의 JSON 필드 검증과 품질 엔진의 정규화를 재사용하므로 별도의 검증·정제 태스크에서 같은 작업을 반복하지 않습니다.

품질 검사에서 행별 결과는 메모리에서 계산하고 XCom에는 요약만 반환합니다. 분리 적재에서는 같은 RAW를 다시 평가해 선행 검사와 배치·규칙 버전·해시·집계가 일치하는지 **DB 쓰기 전** 비교합니다. 판정 로직은 하나이며 두 태스크에서 호출합니다. 입력과 결과를 전달할 외부 artifact 저장소 없이 1,000행 로컬 프로젝트를 연결하기 위한 선택입니다. DAG는 별도 JSON 보고서 파일을 갱신하지 않습니다.

## 실행 정책

- `schedule=None`: 수동 실행. 아직 새 일자 입력을 만드는 정기 수집원이 없어 자동 스케줄을 시작하지 않습니다.
- `catchup=False`: 자동 과거 실행을 만들지 않습니다. 날짜 범위 백필 정책은 후속 범위입니다.
- `max_active_runs=1`, `max_active_tasks=1`: 이 DAG의 실행과 태스크 동시성을 제한합니다. 수동 CLI 적재는 이 제한 밖에 있으므로 실행 중 같은 배치 수동 재처리는 피합니다.
- 기본 retry 2회(최초 시도 포함 최대 3회), 첫 지연 30초, exponential backoff, 최대 지연 2분.
- 태스크 timeout 5분, DAG run timeout 20분. 학습용 소규모 입력 기준입니다.
- `all_success` 기본 의존성: 앞 태스크가 최종 실패하면 후속 태스크는 실행되지 않습니다.
- 경로·스키마·해시·빈 입력·메타데이터 불일치 등 결정적 오류는 `AirflowFailException`으로 재시도 없이 실패합니다. DB 접속 등 인프라 예외는 기본 재시도 대상입니다.
- 레코드 ERROR는 격리하고 WARNING은 보존합니다. 격리 행이 존재한다는 이유만으로 DAG를 실패시키지 않습니다. 배치 품질 임계치 게이트는 Phase 7입니다.

## 입력과 재실행

`input_file`은 `/opt/airflow/project/data` 기준 상대 경로입니다. 기본값은 `generated/products.jsonl`입니다. 절대 경로, data 밖으로 벗어나는 경로, JSONL 외 확장자, `*.labels.jsonl`은 거부합니다. 입력 옆에 같은 stem의 `*.manifest.json`이 있어야 합니다. 입력 파일은 실행 중 변경하지 않습니다. 여러 입력을 보존할 때는 배치별 고유 파일 경로를 사용합니다.

RAW 적재는 기존 배치·해시·건수 정책을 유지합니다. 결과의 처리 UUID는 DAG ID·실제 run ID·batch ID로 UUID5를 계산합니다.

- 같은 DAG run의 task 재시도/clear: 같은 UUID. 현재 성공 결과가 존재하면 `already_processed`로 반환합니다.
- 새로운 DAG run: 새 UUID. 최신 결과를 교체하고 성공 요약 이력 1개를 추가합니다.
- 다른 실행이 결과를 교체한 뒤 이전 run을 clear: 이전 UUID는 superseded 상태라 적재를 거부합니다. 새 DAG run으로 재처리합니다.
- task retry 뒤 RAW가 성공해도 최종 적재가 실패할 수 있습니다. RAW는 보존되며 각 적재 트랜잭션은 독립입니다. 네 태스크 전체가 하나의 DB 트랜잭션인 것은 아닙니다.

결과·이슈와 `clean.processing_run.dag_run_id`에는 실제 Airflow run ID를 기록합니다. 실패 태스크 이력·로그는 Airflow가 관리하며 DB 처리 요약에는 성공 적재만 저장합니다.

## WSL 실행 방법

이미 준비된 환경에서:

```bash
cd "/mnt/c/Users/dwjam/OneDrive/문서/ChatGPT/arirflowETL프로젝트"
docker compose up -d

# 입력이 없을 때 생성. 실행 중인 DAG의 파일을 덮어쓰지 않습니다.
python3 src/generator/generate_product_data.py

# 새 파일 등록은 DAG processor의 주기적 검색을 기다립니다.
docker compose exec -T airflow airflow dags list
docker compose exec -T airflow airflow dags list-import-errors

# DAG unpause 후 실제 스케줄러 실행을 요청합니다.
bash scripts/trigger_pipeline.sh

# 다른 준비된 입력도 상대 경로로 지정할 수 있습니다.
# bash scripts/trigger_pipeline.sh generated/another-batch.jsonl

docker compose exec -T airflow airflow dags list-runs ai_output_quality_pipeline -o json
```

UI: `http://localhost:8080` → `ai_output_quality_pipeline` → 해당 run → Graph/Grid → 태스크 → Logs. UI Trigger 화면의 `input_file`도 같은 상대 경로입니다. 실행 버튼을 누르기 전에 DAG가 unpaused 상태인지 확인합니다.

run ID를 복사해 해당 실행의 태스크 상태를 조회합니다:

```bash
docker compose exec -T airflow airflow tasks states-for-dag-run \
  ai_output_quality_pipeline '<실제 run ID>' -o json

docker compose exec -T project-postgres psql -X -U etl_user -d etl_project \
  < sql/etl/007_inspect_results.sql
```

`load_results` 로그의 처리 UUID·run ID·건수와 DB 처리 이력을 연결해 볼 수 있습니다. 로그와 XCom에 원본 행이나 비밀번호를 출력하지 않습니다. 태스크는 기존 제한 계정 `etl_app`으로 접속하며 관리자 DDL을 수행하지 않습니다.

## 실제 실행 기록

스케줄러의 실제 수동 DAG 실행 1회를 수행했습니다. `dags test`·단위 테스트·장애 주입은 수행하지 않았습니다.

- Run ID: `manual__2026-09-28T06:58:12.656512+00:00`
- 실행 시각: 2026-09-28 15:58:13~15:58:19 KST (UTC 06:58:13~06:58:19).
- 처리 UUID: `8249849a-e3e5-5948-83e9-0a2732392002`.
- 배치: `batch-a2ebfbe5e814`, 규칙 1.1.
- DAG 상태: `success`. 네 태스크 모두 최초 시도에서 `success`.
- 기본 입력은 이미 RAW에 있던 배치여서 수집·적재 단계는 기존 배치를 확인하고 재사용했습니다. 새 RAW 배치 최초 적재를 DAG에서 시연한 것은 아닙니다.

| DB 조회 항목 | 결과 |
|---|---:|
| CLEAN | 959행 |
| QUARANTINE | 41행 |
| 이슈 | 44개 |
| CLEAN에 연결된 WARNING | 3개 |
| CLEAN/QUARANTINE 중복 | 0개 |
| QUARANTINE 업무 값·원천 적재 시각의 RAW 대비 차이 | 0개 |
| RAW 전체 | 2,000행 |
| 성공 요약 이력 | 기존 수동 2개 + DAG 1개 = 3개 |
| 해당 배치 최신 성공 이력 | 1개 |

첫 실행 로그의 deprecated `airflow.exceptions.AirflowFailException` 안내를 확인해 `airflow.sdk.exceptions.AirflowFailException`으로 갱신했습니다. 실제 결과 적재 로그에는 위 run ID·처리 UUID·959/41/44 요약을 기록했습니다. 코드 갱신 후 별도 DAG 재실행은 수행하지 않았습니다.

## 주요 파일

- `airflow/dags/ai_output_quality_pipeline.py`: TaskFlow DAG·의존성·Params·재시도·timeout.
- `src/etl/pipeline.py`: 작은 태스크 데이터 계약·입력 수집·기존 함수 연결.
- `src/load/load_raw.py`: 수집한 입력 identity를 RAW 쓰기 전에 비교하는 선택 인자 추가. 기존 CLI 유지.
- `src/etl/process_batch.py`: 선행 품질 요약을 트랜잭션 안에서 재평가 값과 비교하는 선택 인자 추가. 기존 CLI 유지.
- `scripts/trigger_pipeline.sh`: WSL에서 안전하게 JSON conf를 만들어 unpause·trigger.

## 다음 단계와 한계

Phase 7에서 날짜·카테고리·모델 버전별 품질 마트와 임계치 게이트를 붙입니다. 현재 success는 네 태스크의 완료를 의미하며 배치 임계치 검사를 통과했다는 뜻은 아닙니다.

DB 중단·스키마 오류·입력 변경·timeout·재시도·동시 실행·실패 롤백의 실제 장애 시연 및 과거 run clear 시나리오는 Phase 8에 남습니다. 대용량 데이터에서는 지금의 메모리 판정과 중복 평가 대신 결과 artifact나 별도 처리 전략을 검토해야 합니다. `standalone` 환경은 로컬 개발용입니다.

## 참고

- [Airflow 3 TaskFlow 공식 문서](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
- [Airflow Task SDK 공식 API](https://airflow.apache.org/docs/task-sdk/stable/api.html)
