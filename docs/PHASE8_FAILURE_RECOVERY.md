# Phase 8 — 장애·재실행·복구 시연

기준일: 2026-09-28. 검증 완료. 기존 기능과 정책 1.0을 검증하며 실무 확장·새 품질 정책은 추가하지 않았습니다.

확정 묶음 `20260928T075054`은 16:50:54~16:56:44 KST에 실행해 `status=PASS`로 완료했습니다. 보존 확인·clear 선택 확인을 포함한 증거 이벤트 32개를 기록했고 실제 DAG run은 13개입니다. 아래 실패들은 시나리오에서 기대한 실패입니다.

준비 작업 복구 후 읽기 전용 최종 checker도 PASS였습니다. 최종 DAG 상태는 성공 7개·의도한 실패 6개이며 기존 스냅샷·서비스 상태를 다시 확인했습니다. 증거: 같은 폴더의 `final-check.json`.

## 방법과 입력 보존

- 실제 Airflow 스케줄러·LocalExecutor·프로젝트 PostgreSQL을 사용했습니다. DAG의 여섯 태스크와 제한 계정 `etl_app`을 그대로 사용합니다.
- 확정 실행 묶음: `20260928T075054`. 입력·manifest·기계 판독 증거: `data/generated/phase8/20260928T075054/`.
- 각 유효 실험 배치는 100행이며 기존 생성기의 정상 입력을 복사해 지정한 오류를 주입했습니다. 이 입력들은 정답 라벨을 파이프라인에 넣지 않습니다.
- 초기 두 배치 `batch-208ca532eb1f`, `batch-a2ebfbe5e814`는 별도 실험 ID로부터 분리했습니다. 검증 전후 원본·결과·이력·마트·게이트의 행 내용 SHA-256을 비교합니다.
- PostgreSQL 중지 실험은 `project-postgres` 서비스만 중지하며 Airflow는 실행합니다. 러너는 `finally`에서 DB를 다시 시작합니다. 볼륨 삭제는 하지 않습니다.
- Python assertion, DB 스냅샷, DAG 상태와 attempt 로그를 조합해 기대 결과를 확인합니다. 의도한 DAG 실패는 실험 실패가 아닙니다.

## 시나리오별 확인

| 시나리오 | 실제 기대 결과와 확인 |
|---|---|
| 신규 정상 배치 | RAW 최초 적재·여섯 태스크 성공, 100행 통과 |
| 한 행의 속성 NULL | 99행 통과·1행 격리, 1% 게이트 PASS |
| 정확히 5% | 95행 통과·5행 격리, PASS |
| 6% | 94행 통과·6행 격리, 마지막 게이트 실패, RAW·결과·마트·FAIL 이력 보존 |
| 가상 모델 v1.1의 NULL 20% | 80행 통과·20행 격리, 마지막 게이트 실패·조사 데이터 보존 |
| 실패 게이트 재실행 | 같은 UUID·정책으로 다시 FAIL, 기존 판정과 데이터는 그대로 |
| 수정 입력 새 배치 | 별도 corrected 배치 PASS, 이전 bad-model RAW 유지 |
| NULL/빈 차원·시각 결측 | NULL과 빈 문자열 별도 그룹, 생성 시각 결측 2행을 적재 UTC 날짜로 집계 |
| 시간대 변환 | `2026-08-31T23:30:00-01:00`을 UTC 2026-09-01로 집계 |
| 필수 category 컬럼 누락 | `collect_input` 즉시 실패, 후속 `upstream_failed`, 해당 RAW 0행 |
| 빈 입력 | `collect_input` 즉시 실패, RAW 배치 이력도 생성하지 않음 |
| 같은 batch ID 다른 파일 내용 | 수집은 통과, `load_raw` 실패, 기존 전체 데이터 스냅샷 유지 |
| 같은 run의 태스크 clear | load_results·마트·게이트를 실제 다시 실행, 처리 UUID·성공 처리/게이트 이력 증가 없음 |
| 새 run 재처리 | RAW 불변, 결과·마트 건수 유지, 새 성공 처리 이력 1개·현재 표시 1개 |
| 이전 run이 superseded된 뒤 clear | `load_results` 실패·후속 차단, 현재 결과 스냅샷 유지 |
| 분리 적재 중간 실패 | 실제 삭제·삽입 후 commit 전 예외, 이전 결과·이력·마트·게이트 스냅샷 복원 |
| 마트 중간 실패 | 실제 마트 교체·규칙 삽입 후 commit 전 예외, 이전 마트·규칙·결과 스냅샷 복원 |
| 같은 배치·UUID 동시 처리 | 두 연결을 barrier로 동시에 출발시킴. `processed` 1개·`already_processed` 1개, 새 성공 이력 1개 |
| 마트 무효화 | 동시 처리의 새 결과가 이전 마트를 삭제함을 조회, 새 마트·게이트로 재구축 |

## DB 장애와 복구

1. `phase8__20260928T075054__db-transient`: `load_raw` attempt=1 실패·`up_for_retry` 확인 후 DB 시작. attempt=2에서 성공하고 DAG 전체 성공.
2. `phase8__20260928T075054__db-exhausted`: DB 중지를 유지해 attempt=1/2/3 실패. `load_raw=failed`, 나머지 후속 태스크 `upstream_failed` 확인.
3. DB 복구 후 실패 run의 `load_raw`부터 후속 5개 태스크만 REST API로 clear. attempt=4에서 성공하고 전체 DAG 성공. RAW hash/행 수 불변, 새 성공 처리 이력 1개·최신 표시 1개 확인.

두 경우 모두 프로젝트 PostgreSQL만 중지했습니다. Airflow의 메타데이터/API/스케줄러는 계속 실행됐고 종료 후 PostgreSQL healthy·Airflow running을 확인했습니다.

## 환경 준비 스크립트의 실패 복구

`scripts/check_setup_recovery.py`도 실제 WSL Compose·관리자 DB 마이그레이션을 사용해 완료했습니다. 주입 위치는 검증 프로세스의 Python 호출 경계입니다.

- SQL 호출이 commit되기 전 실패를 주입: 이전 `.env` 유지, 기존 서비스 상태 복원, 임시 설정 제거.
- 실제 DB 마이그레이션 커밋 후 설정 publication 실패 주입: staged 비밀 없는 설정 보존, 기존 secret 파일을 유지해 준비 스크립트 재실행으로 forward recovery 성공.
- `.env` 내용·비밀파일 내용이 전후 동일하고 서비스가 모두 실행 중임을 확인했습니다. 테스트가 만든 staged 설정만 복구 후 제거했습니다.
- 증거: `data/generated/phase8/20260928T075054/setup-recovery.json`, `status=PASS`.

이 실험은 Docker 엔진 자체 장애나 복구 명령마저 실패하는 경우까지 검증한 것은 아닙니다.

## 최종 데이터 보존

| 구분 | RAW 행 |
|---|---:|
| 기존 초기 배치 | 1,000 |
| 기존 기본 배치 | 1,000 |
| 중단한 예비 실험 6개 배치 | 600 |
| 확정 실험 7개 배치 | 700 |
| 전체 | 3,300 |

기존 두 배치의 RAW·결과·성공 이력·마트·게이트 전체 행 내용은 전후 SHA-256 비교에서 동일했습니다. 기존 기본 배치의 959/41/44와 마트 5그룹·정책 1.0 PASS도 유지했습니다. 새 실험 데이터를 추가했으므로 전체 RAW 행 수가 2,000→3,300으로 늘었고 중복 적재 때문은 아닙니다. 스키마 누락·빈 입력·내용 충돌 실험은 새 RAW를 만들지 않았습니다.

최종 normal run은 superseded clear 시나리오에서 의도적으로 실패 상태로 남습니다. bad-model/6%도 실패가 정상 기대 결과입니다. 정상 복구를 보려면 `db-transient`, `db-exhausted`, `corrected` run을 확인합니다.

## 롤백·동시성 검증의 범위

`src/validation/phase8_probe.py`에서 Python 함수/커서를 일시적으로 patch해 commit 전 오류를 주입했습니다. 삭제와 삽입은 실제 DB에 수행했으며 기존 행 전체 스냅샷이 동일하게 복원됐습니다. 운영 DAG에 장애 스위치나 임시 SQL trigger를 추가하지 않았습니다.

동시성 실험은 동일 배치·동일 처리 UUID의 두 스레드·두 실제 DB 연결입니다. DAG의 동시 run 제한을 우회하는 수동 함수 호출 상황을 검증했습니다. RAW loader와 결과 loader의 혼합 경쟁, 서로 다른 UUID 대량 경쟁, DB 프로세스 강제 종료에 따른 crash recovery까지 검증한 것은 아닙니다. 롤백으로 sequence 번호가 소비되어 간격이 생기는 것은 행 데이터 불변성과 별개입니다.

## 재실행 절차 개선

초기 실행 묶음 `20260928T074719`에서 CLI의 날짜·task regex 필터를 사용한 clear 호출이 실제 재시도를 만들지 않았습니다. attempt=1만 있는 것을 확인해 검증을 중단했습니다. 해당 묶음의 clear/멱등성 판정은 확정 증거로 사용하지 않습니다. 이미 생성한 가상 입력·RAW·로그는 남겨 원본을 임의 삭제하지 않았습니다.

최종 러너는 REST API `clearTaskInstances`를 사용합니다. 명시적 `dag_run_id`와 task ID 목록으로 dry-run을 먼저 조회해 **선택된 run과 태스크가 정확한지 확인**한 후 실제 clear합니다. 재실행 로그의 attempt 증가까지 assertion으로 확인합니다. 이 테스트 helper는 `phase8__` 접두사의 run만 clear할 수 있습니다. 로컬 관리자 암호·토큰은 컨테이너 안에서 읽고 사용하며 증거·로그·문서에 저장하지 않습니다.

원인 조사와 구현 참고: [Airflow REST API 공식 문서](https://airflow.apache.org/docs/apache-airflow/3.2.2/stable-rest-api-ref.html). 설치된 3.3.2 코드와 실제 API 응답도 확인했습니다. CLI 일반 기능 전체가 동작하지 않는다고 판단한 것은 아닙니다.

## WSL 재현

이 명령은 새 실험 배치들을 추가하고 실제 DB 중지를 수행하므로 프로젝트의 다른 실행이 없을 때 사용합니다. 중간에 종료할 때는 Ctrl+C를 사용해 `finally` 복구가 실행되도록 합니다. 강제 kill·WSL 종료까지 자동 복구를 보장하지는 않습니다.

```bash
cd "/mnt/c/Users/dwjam/OneDrive/문서/ChatGPT/arirflowETL프로젝트"
docker compose ps
python3 -u scripts/run_phase8.py

# 준비 스크립트의 commit 전후 실패 복구 확인
# EVIDENCE를 이번 러너가 출력한 실제 경로로 바꿉니다.
EVIDENCE='data/generated/phase8/이번_suite_id/evidence.json'
python3 scripts/check_setup_recovery.py "$(dirname "$EVIDENCE")/setup-recovery.json"

# 다른 배치를 적재하기 전에 이번 증거 확인
python3 scripts/check_phase8_final.py "$EVIDENCE"

docker compose ps
docker compose exec -T airflow airflow dags list-runs ai_output_quality_pipeline -o json
```

복구가 필요한 경우:

```bash
docker compose start project-postgres
docker compose exec -T project-postgres pg_isready -U etl_user -d etl_project
```

## 증거와 남은 범위

- 확정 증거 JSON: `data/generated/phase8/20260928T075054/evidence.json`. 입력과 증거는 generated Git ignore 대상입니다.
- 준비 복구 `setup-recovery.json`, 최종 확인 `final-check.json`도 확정 실행 폴더에 저장했습니다. 위 재현 명령은 이번 실행 폴더에 저장하며, 경로 인자 없이 준비 checker를 실행하면 기본 출력은 `data/generated/phase8/setup-recovery.json`입니다.
- 태스크 상세 로그: Airflow 볼륨 `/opt/airflow/logs/dag_id=ai_output_quality_pipeline/run_id=phase8__20260928T075054__<시나리오>/task_id=<태스크>/attempt=<번호>.log`.
- 로그 summary에는 run ID·batch·집계·판정·attempt를 저장하고 원본 행·비밀 값을 저장하지 않습니다.
- 실제 태스크 timeout 5분 소진, 대용량 부하, crash recovery, 이미지 CVE·침투 검증은 이번 범위에 포함하지 않았습니다. 전역 5% 게이트의 운영 기준 적합성 검토도 프로젝트 완성 후 확장 범위입니다.
- 이후 Phase 9 문서 정리를 완료했습니다. 최종 절차는 [RUNBOOK](RUNBOOK.md), 완료 기준과 한계는 [최종 보고](PHASE9_FINAL_REPORT.md)에 있습니다. 실무 확장은 별도 선택 범위입니다.
