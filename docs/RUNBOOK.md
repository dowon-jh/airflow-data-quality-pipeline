# 실행·재현 안내서

기준일: 2026-09-28. 모든 명령은 **Ubuntu WSL의 Bash**에서 실행합니다. 기본 버전은 수동 실행이며 실제 LLM을 호출하지 않습니다.

## 1. 준비 조건

- Windows의 Docker Desktop 실행, Ubuntu WSL 2 설치, Docker Desktop의 Settings → Resources → WSL Integration에서 Ubuntu 연동 활성화.
- Ubuntu에서 `docker version`, `docker compose version`, `python3 --version` 실행 가능. 호스트 Python은 3.10 이상 문법을 사용하며 현재 확인 버전은 3.14.4입니다. 최소 버전별 실행 검증은 하지 않았습니다.
- 프로젝트 폴더에 `docker-compose.yml`, `scripts/`, `src/`, `sql/`, `airflow/dags/`가 있어야 합니다. 호스트 Python에는 별도 pip 설치가 필요하지 않습니다.
- 첫 준비에는 컨테이너 이미지 다운로드가 필요합니다. 로컬 8080 포트가 다른 프로그램에서 사용 중이면 먼저 해당 사용 상태를 확인합니다.

GitHub에서 처음 준비하는 경우:

```bash
git clone https://github.com/dowon-jh/airflow-data-quality-pipeline.git
cd airflow-data-quality-pipeline
```

현재 개발 PC 경로는 다음과 같습니다. 이미 위 clone 폴더로 이동했다면 아래 cd는 실행하지 않습니다.

```bash
cd "/mnt/c/Users/dwjam/OneDrive/문서/ChatGPT/arirflowETL프로젝트"
docker version
docker compose version
python3 --version
```

## 2. 처음 준비 또는 기존 환경 업데이트

```bash
python3 scripts/setup_local.py
docker compose ps
docker compose exec -T project-postgres pg_isready -U etl_user -d etl_project
```

**새 환경에서는 setup을 먼저 실행합니다.** 이 스크립트가 비밀파일과 `.env`를 준비하고 PostgreSQL을 시작해 모든 DDL·실행 계정 권한을 적용한 다음 Airflow를 시작합니다. `.env.example`의 다른 사용자 홈 경로를 그대로 복사할 필요가 없습니다.

DB 암호는 현재 WSL 사용자의 `~/.config/ai-output-dq-pipeline/secrets/`에 보관합니다. 현재 PC에서는 `/home/dwjam/.config/ai-output-dq-pipeline/secrets/`입니다. `.env`에는 비밀 값 대신 경로와 일반 설정을 저장합니다. 기존 DB 볼륨이 있는데 원래 비밀파일이 없으면 setup은 새 암호를 임의로 만들지 않고 중단합니다.

기대 상태는 PostgreSQL `healthy`, Airflow 실행 중입니다. Airflow 기동·DAG 검색은 추가 시간이 필요합니다. UI 접속 실패 시 로그를 확인합니다.

```bash
docker compose logs --tail=100 airflow
docker compose logs --tail=100 project-postgres
```

## 3. 로그인과 DAG 등록 확인

브라우저에서 `http://localhost:8080`을 열고 사용자 **admin**으로 로그인합니다. 비밀번호는 로컬에서만 확인합니다.

```bash
docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
docker compose exec -T airflow airflow dags list
docker compose exec -T airflow airflow dags list-import-errors -o json
docker compose exec -T airflow airflow tasks list ai_output_quality_pipeline
```

`ai_output_quality_pipeline`이 목록에 있고 import 오류가 없어야 합니다. 기동 직후 목록에 없다면 잠시 후 다시 조회합니다. 여섯 태스크는 `collect_input → load_raw → evaluate_quality → load_results → build_quality_metrics → check_batch_thresholds`입니다. 암호 파일이 아직 없다면 Airflow 초기 기동 로그부터 확인합니다.

## 4. 기본 입력 생성과 전체 파이프라인 실행

```bash
python3 src/generator/generate_product_data.py
cat data/generated/products.manifest.json
bash scripts/trigger_pipeline.sh
docker compose exec -T airflow airflow dags list-runs ai_output_quality_pipeline -o json
```

생성기 1.1의 기본 설정은 1,000행·seed 42·오류 주입 4%·속성 NULL 1%·가상 모델 v1.0입니다. 입력·manifest·라벨을 생성하지만 라벨은 DAG 입력으로 사용하지 않습니다. 입력 생성은 DAG 밖에서 수행하며 **실행 중 코드·입력·manifest를 변경하지 않습니다.**

trigger 출력의 `run_id`를 복사합니다. trigger 명령 성공은 실행 접수이며 여섯 태스크의 완료를 뜻하지 않습니다. UI의 해당 run Grid/Graph·로그를 보거나 아래 명령으로 확인합니다.

```bash
# 아래 값을 방금 출력된 실제 run_id로 바꿉니다.
RUN_ID='여기에_실제_run_id'
docker compose exec -T airflow airflow tasks states-for-dag-run \
  ai_output_quality_pipeline "$RUN_ID" -o json
```

기본 입력에서는 여섯 태스크와 run이 `success`가 됩니다. 직접 만든 JSONL을 실행할 때는 data 아래 상대 경로를 넘깁니다. 같은 stem의 manifest도 필요합니다.

```bash
bash scripts/trigger_pipeline.sh generated/products.jsonl
```

## 5. 기본 배치 결과 조회

아래 조회는 **기본 배치만** 대상으로 하므로 이전 장애 실험 데이터가 있어도 기준 결과를 읽을 수 있습니다.

```bash
docker compose exec -T project-postgres psql -X -U etl_user -d etl_project <<'SQL'
SELECT batch_id, COUNT(*) AS raw_rows
FROM raw.ai_product_output WHERE batch_id='batch-a2ebfbe5e814' GROUP BY batch_id;

SELECT batch_id,input_rows,clean_rows,quarantine_rows,warning_rows,issue_count,
       ruleset_version,processing_run_id,dag_run_id
FROM clean.processing_run WHERE batch_id='batch-a2ebfbe5e814' AND is_current;

SELECT metric_date,category,model_version,input_rows,clean_rows,quarantine_rows,
       warning_rows,null_attribute_rows,duplicate_id_rows,evidence_candidate_rows
FROM mart.current_daily_quality_metrics
WHERE batch_id='batch-a2ebfbe5e814' ORDER BY category;

SELECT g.dag_run_id,g.policy_version,g.input_rows,g.quarantine_rows,
       ROUND(g.actual_quarantine_rate*100,2) AS quarantine_percent,g.decision
FROM mart.batch_quality_gate g JOIN clean.processing_run r
  USING (processing_run_id,batch_id)
WHERE g.batch_id='batch-a2ebfbe5e814' AND r.is_current;
SQL
```

| 항목 | 기본 설정의 기대 결과 |
|---|---:|
| batch_id | `batch-a2ebfbe5e814` |
| RAW / CLEAN / QUARANTINE | 1,000 / 959 / 41 |
| 경고 행 / 전체 이슈 | 3 / 44 |
| 최신 마트 그룹 | 5 (UTC 2026-09-01, v1.0) |
| NULL 속성 / 중복 ID 행 / 근거 후보 행 | 10 / 8 / 3 |
| 배치 격리율 / 정책 1.0 판정 | 4.1% / PASS |

경고 3행은 CLEAN에 포함됩니다. 중복의 원래 행도 격리하며 `40 + 4 - 3 = 41`이라 오류 주입 수와 격리 행 수는 다릅니다. 이슈 44개와 격리 41행도 다른 지표입니다.

전체 배치·이력을 조회하려면:

```bash
docker compose exec -T project-postgres psql -X -U etl_user -d etl_project \
  < sql/etl/007_inspect_results.sql
docker compose exec -T project-postgres psql -X -U etl_user -d etl_project \
  < sql/mart/009_inspect_quality_mart.sql
```

**새 설치의 기본 실행만 하면 RAW 전체는 1,000행**입니다. 현재 PC의 3,300행은 초기 배치와 Phase 8 실험까지 누적한 기록입니다. 같은 입력 재실행은 RAW를 늘리지 않지만 새 DAG run은 새 처리·게이트 이력을 만듭니다. run ID·UUID·실행 시각은 새 환경에서 달라집니다.

## 6. 실패 원인과 복구

먼저 UI에서 실패한 run의 최초 실패 태스크 로그를 읽습니다. 후속 `upstream_failed`는 선행 실패의 결과일 수 있습니다.

| 상황 | 복구 순서 |
|---|---|
| `service airflow is not running` | 처음이면 setup 실행, 준비된 환경이면 `docker compose up -d` 후 상태·로그 확인 |
| DAG 등록/import 오류 | 기동 로그와 import 오류 확인 → 코드/설정 수정 → DAG 등록 확인 후 실행 |
| DB 일시 연결 실패 | DB 상태 복구; 자동 재시도 2회(최대 3시도)의 결과 확인 |
| DB 장애로 재시도 소진 | `docker compose up -d project-postgres` → healthy 확인 → 아래 특정 run 복구 절차 |
| 필드 누락·빈 입력·manifest 불일치 | 입력과 manifest 수정·재생성 → 새 run 실행; 이미 RAW에 저장된 입력을 수정하면 새 batch_id 사용 |
| 같은 batch_id에 다른 내용 | 기존 RAW 보존 → 수정 입력에 새 batch_id와 일치하는 manifest 생성 → 새 run |
| 격리율 5% 초과 | 결과·이슈·마트·FAIL 판정 조사 → 원인 수정 입력을 새 배치로 생성 → 새 run; 동일 입력 반복으로 PASS가 되지는 않음 |
| 이전 run이 superseded 오류 | 배치가 더 새 실행으로 교체됨; 이전 run clear 대신 새 run 사용 |
| setup 실패 | 출력된 commit 전/후 복구 안내 확인 → 원인 해결 → setup 재실행; 비밀파일 삭제/재생성 금지 |

특정 run 복구는 UI에서 **정확한 DAG·run·실패 태스크**를 선택하고 Clear의 대상 목록을 확인합니다. 후속 태스크도 다시 실행해야 합니다. 예를 들어 RAW 단계 실패면 `load_raw`부터 게이트까지 선택합니다. 다른 run·과거/미래 실행이 포함되지 않는지 확인한 뒤 clear하고 attempt 증가·완료 결과를 확인합니다. 이미 새 run으로 대체된 run에는 사용하지 않습니다.

Phase 8 예비 실험의 날짜 범위 CLI clear는 실제 attempt 증가를 만들지 못했습니다. 확정 실험은 명시적 run/task REST clear를 사용했습니다. 검증 helper는 `phase8__` run만 허용하므로 일반 run 복구에는 위 UI 절차를 사용합니다.

**게이트 FAIL에서도 CLEAN·QUARANTINE·마트가 저장되어 있습니다.** 데이터 저장 성공과 품질 승인 성공은 별개입니다. 소비자는 최신 처리 UUID에 연결된 게이트 PASS를 확인해야 합니다. 자동 외부 배포 차단 기능은 구현하지 않았습니다.

## 7. 장애 시연 재현 — 선택 절차

기본 실행을 완료한 후 다른 DAG 실행이 없을 때 수행합니다. 이 절차는 실제 프로젝트 DB를 잠시 중지하고 실험 배치를 추가합니다. 예상 실패 run은 UI에 남습니다.

```bash
python3 -u scripts/run_phase8.py
```

러너가 출력한 **이번 실행의 evidence.json 경로**를 아래 변수에 넣습니다. 이전 확정 묶음 경로를 재사용하면 이번 실험을 확인하는 것이 아닙니다.

```bash
EVIDENCE='data/generated/phase8/이번_suite_id/evidence.json'
python3 scripts/check_setup_recovery.py "$(dirname "$EVIDENCE")/setup-recovery.json"
python3 scripts/check_phase8_final.py "$EVIDENCE"
```

다른 배치 적재 전에 마지막 checker를 실행합니다. 이 checker는 해당 증거에 기록된 DB 전체 건수와 기준 배치 스냅샷을 비교합니다. 러너 PASS·준비 복구 PASS·final-check PASS를 확인합니다. 확정 사례의 13 run은 최종 7개 success·6개 의도한 failed였으며, 테스트 성공과 모든 DAG 성공은 다른 기준입니다.

원래 증거는 `data/generated/phase8/20260928T075054/`입니다. 주입 방식·범위·기대 결과는 [Phase 8 기록](PHASE8_FAILURE_RECOVERY.md)을 참고합니다. Phase 9에서는 이 장애 시연을 새로 실행하지 않았습니다.

## 8. 시작·종료·보관

```bash
# 이미 준비된 환경 시작
docker compose up -d
docker compose ps

# 종료: named volume은 보존
docker compose down
```

볼륨 삭제는 DB 데이터와 Airflow 실행 이력을 지웁니다. 이 안내서는 데이터 초기화를 요구하지 않습니다. 기존 볼륨을 재사용할 때 원래 WSL 비밀파일도 함께 유지해야 합니다.

프로젝트를 전달할 때 코드·SQL·DAG·문서를 전달합니다. `.env`·DB 암호·Airflow 로그인 암호를 문서나 저장소에 넣지 않습니다. `data/generated/`와 증거 JSON은 Git ignore 대상이므로 **실제 실행 증거를 전달하려면 별도로 보관**합니다. 새 사용자는 setup으로 자기 환경을 준비하고 생성기로 기본 입력을 재생성합니다. 폴더 복사만으로 Docker 볼륨·과거 실행 이력까지 복제되지는 않습니다. DB 백업·복원 시연은 별도 미수행입니다.

개별 품질 보고서는 `bash scripts/evaluate_quality.sh`로 생성하며 DAG는 이 파일을 갱신하지 않습니다. `bash scripts/process_batch.sh`는 분리 적재만 수행하고 이전 마트를 무효화합니다. 전체 마트·게이트까지 사용할 때는 기본 DAG 절차를 따릅니다.

## 문서 검증 범위

이 순서는 현재 코드·Compose·SQL·실행 증거에 맞춰 작성했습니다. 이후 같은 PC에서 별도 폴더·새 설정·새 비밀파일·빈 DB/Airflow 볼륨을 만들어 setup부터 기본 DAG까지 실행했고 기대 결과와 일치했습니다. 기존 DB·설정·비밀파일·서비스 상태도 전후 동일했습니다. [빈 환경 재현 확인](CLEAN_INSTALL_VERIFICATION.md).

재현 확인 명령은 기존 기본 환경이 실행 중이고 8081 포트가 비어 있을 때 원래 프로젝트 루트의 WSL에서 `python3 -u scripts/check_clean_install.py`입니다. 새 환경은 매번 별도 이름을 쓰며 성공 후 서비스를 중지하고 결과를 보존합니다. 사용자 지정 환경에서 setup을 다시 실행하려면 최초 준비와 같은 `PROJECT_COMPOSE_NAME`·`PROJECT_AIRFLOW_PORT` 환경변수를 지정합니다.

**별도 PC나 새 OS 설치 검증은 수행하지 않았습니다.** Docker Desktop·WSL·Python 설치와 이미지 다운로드 캐시는 기존 것을 사용했습니다.
