# Phase 7 — 품질 마트와 임계치 게이트

기준일: 2026-09-28. 구현 및 기본 배치 실제 DAG 실행 완료. 장애·재시도·임계치 초과 시연은 Phase 8에 남습니다.

후속 상태: 위 문장은 Phase 7 시점 기록입니다. 이후 [Phase 8](PHASE8_FAILURE_RECOVERY.md)에서 임계치·입력 오류·DB 중단·재시도·롤백·동시 처리·복구를 실제 시연했습니다.

## 처리 흐름

```text
collect_input → load_raw → evaluate_quality → load_results
    → build_quality_metrics → check_batch_thresholds
```

`build_quality_metrics`는 저장된 RAW·CLEAN·이슈로 품질 점수판을 만듭니다. `check_batch_thresholds`는 마트와 성공 처리 이력의 건수가 일치하는지 확인한 뒤 배치 전체의 격리율을 판단합니다. 각 태스크의 인프라 재시도·timeout·의존성은 Phase 6 정책을 유지합니다.

## 집계 기준

- 차원: `batch_id`, `metric_date`, `category`, `model_version`.
- 날짜: RAW `created_at`의 UTC 날짜. 결측이면 `ingested_at` UTC 날짜를 사용하고 `missing_created_at_rows`에 기록합니다. 재실행 시각으로 날짜를 바꾸지 않습니다.
- 카테고리: RAW를 NFKC·공백 정리·대문자로 정규화. 모델 버전: NFKC·공백 정리, 대소문자 유지.
- NULL과 빈 문자열은 서로 다르게 보존합니다. 임의의 문자 sentinel로 대체하지 않습니다.
- 잘못된 카테고리 `UNKNOWN`도 별도 그룹으로 집계합니다. 오류 행을 집계에서 제외하지 않습니다.
- 같은 날짜·카테고리·모델이라도 배치가 다르면 다른 행입니다. 여러 배치를 비교할 때는 행 수를 합친 뒤 비율을 계산하며 그룹 비율의 단순 평균을 사용하지 않습니다.

## 저장 구조

| 테이블/뷰 | 내용 | 보존 방식 |
|---|---|---|
| `mart.daily_quality_metrics` | 차원별 건수·비율·처리 UUID | 배치 최신 집계만 저장 |
| `mart.daily_rule_metrics` | 해당 그룹의 규칙·severity별 이슈 수 | 부모 마트에 연결, 교체 시 함께 삭제 |
| `mart.batch_quality_gate` | 처리 UUID·정책 버전·배치·DAG ID·분자/분모·기준·PASS/FAIL·판정 시각 | 실행별 판정 이력 보존 |
| `mart.current_daily_quality_metrics` | 현재 처리 실행에 속한 마트 | 최신 실행만 조회하는 뷰 |

마트의 고유 차원 제약은 PostgreSQL 16의 `UNIQUE NULLS NOT DISTINCT`로 NULL 차원의 중복도 막습니다. 게이트 PK는 `(processing_run_id, policy_version)`입니다. 동일 run의 재시도는 기존 판정과 같은지 확인하며 새 이력을 만들지 않습니다. 판정 시각도 최초 판정 값을 유지합니다. 앱은 판정을 덮어쓰지 않지만 DB 권한 자체가 append-only를 강제하는 것은 아닙니다.

### 주요 지표와 분모

| 지표 | 정의 |
|---|---|
| 입력/통과/격리 행 | 고유 원천 행 기준. 입력 = 통과 + 격리 |
| 경고 행 | WARNING이 1개 이상인 고유 행. 통과 행과 격리 행 모두 포함 가능 |
| NULL 속성 행 | RAW `extracted_attribute IS NULL`. 공백·빈 문자열과 구별 |
| 중복 ID 행 | DQ003에 걸린 행. 중복 그룹 수가 아님 |
| 근거 후보 행 | DQ010에 걸린 행. 확정 환각 판정이 아님 |
| 생성 시각 결측 행 | RAW `created_at IS NULL` |
| 이슈 수 | 모든 규칙의 ERROR/WARNING 합계. 한 행에 여러 이슈가 있을 수 있음 |
| 통과율/격리율/NULL 속성 비율 | 각 지표의 행 수 / 해당 그룹 전체 입력 행 수 |

비율은 NUMERIC generated 컬럼으로 건수와 연결합니다. 빈 배치는 앞 단계에서 거부하므로 저장 그룹의 분모는 항상 양수입니다. 0행 그룹은 만들지 않습니다. 규칙별 상세는 발생 건만 저장하며 상세 행이 없는 규칙은 0건입니다.

## 게이트 정책 1.0

**배치 전체 격리율이 5%를 초과하면 FAIL. 정확히 5%면 PASS.**

기준은 `src/mart/quality.py`의 `MAX_QUARANTINE_RATE = Decimal("0.05")`, 정책 버전은 `1.0`입니다. 이 수치는 정상 합성 배치 4.1%를 기준으로 둔 학습용 고정 기준이며 운영 SLA나 통계적 기준은 아닙니다. UI Params로 임계치를 임의 변경하지 않습니다. 기준을 바꾸면 정책 버전도 올리고 새 DAG run으로 처리합니다.

배치의 `quarantine_rows <= input_rows * 0.05`를 Decimal로 비교합니다. 표시용 반올림 비율로 판정하지 않습니다. 1,000행이라면 50행 격리까지 통과하고 51행부터 실패합니다. NULL 비율·모델별 지표는 마트에 저장하지만 별도의 NULL/그룹 임계치 게이트는 이번 구현에 포함하지 않습니다.

일자·카테고리·모델 그룹은 진단용이며 **그룹별 5% 게이트가 아닙니다**. 이번 입력의 UNKNOWN 그룹은 3행 모두 격리돼 100%이지만 배치 전체 41/1000=4.1%이므로 통과합니다. 특정 모델의 이상이 배치 평균에 가려질 수 있어 그룹 게이트·최소 표본 기준은 향후 확장입니다.

## 트랜잭션과 실패 경계

1. 분리 적재는 기존 마트를 같은 트랜잭션에서 삭제하고 결과·성공 이력을 교체합니다. 이전 지표가 새 결과의 지표처럼 남지 않습니다.
2. 마트 태스크는 RAW 적재·분리 적재와 같은 batch advisory lock을 사용합니다. 현재 run UUID·결과 건수·규칙 집계를 확인하고 마트·규칙 상세를 한 트랜잭션으로 교체합니다.
3. 게이트는 현재 run·마트 UUID·합계 일치를 확인합니다. 누락·다른 run·건수 불일치는 계약 오류로 실패합니다.
4. 게이트 판정을 DB에 커밋하고 함수가 반환한 뒤 DAG 태스크가 FAIL을 `AirflowFailException`으로 표시합니다. 품질 실패는 재시도하지 않습니다. RAW·결과·마트·FAIL 판정은 보존됩니다.

여섯 태스크 전체가 하나의 트랜잭션인 것은 아닙니다. 분리 적재 후 마트 적재가 실패하면 최신 결과는 있으나 마트는 없을 수 있습니다. 해당 run의 마트 태스크부터 복구할 수 있습니다. 다른 실행이 최신 결과를 교체했다면 이전 run의 clear를 거부하므로 새 DAG run을 사용합니다. 수동 CLI 분리 적재도 새 run을 만들면 이전 마트를 지웁니다. 마트가 필요한 경우 전체 DAG를 실행합니다.

`clean.processing_run`의 성공은 분리 적재 완료를 뜻하며 최종 품질 통과와 같지 않습니다. 최종 판단은 `mart.batch_quality_gate`와 Airflow run 상태를 함께 확인합니다. 과거 게이트 판정은 이력으로 유지하되 이전 결과의 전체 행·마트 snapshot을 보존하는 구조는 아닙니다. Phase 6 과거 run에 게이트 결과를 소급해서 만들지 않았습니다.

## 실제 실행 결과

- 실제 DAG run: `manual__2026-09-28T07:16:11.179345+00:00`.
- 시각: 2026-09-28 16:16 KST (UTC 07:16).
- 처리 UUID: `8157c9af-613e-5d28-933f-a5a309332ba7`.
- 배치: `batch-a2ebfbe5e814`, 규칙 1.1, 가상 모델 `v1.0`.
- 여섯 태스크 모두 최초 시도 `success`. 게이트 정책 1.0 판정 `PASS`.
- 준비된 기존 RAW 배치를 재사용했습니다. 새 배치 최초 적재나 나쁜 배치 실패 실험은 수행하지 않았습니다.

| 날짜(UTC) | 카테고리 | 입력 | 통과 | 격리 | 경고 행 | NULL 속성 |
|---|---|---:|---:|---:|---:|---:|
| 2026-09-01 | BAG | 245 | 237 | 8 | 0 | 5 |
| 2026-09-01 | LAMP | 264 | 253 | 11 | 0 | 0 |
| 2026-09-01 | SHOES | 246 | 238 | 8 | 2 | 3 |
| 2026-09-01 | TUMBLER | 242 | 231 | 11 | 1 | 2 |
| 2026-09-01 | UNKNOWN | 3 | 0 | 3 | 0 | 0 |
| 합계 | | 1,000 | 959 | 41 | 3 | 10 |

마트 5행, 규칙별 집계 합계 44개. DQ001~DQ010 합계는 4/4/8/3/10/3/3/3/3/3입니다. 중복 ID 행 8개, 근거 후보 행 3개, 생성 시각 결측 0개입니다. 배치 격리율 4.1% ≤ 5%, NULL 속성 비율은 1.0%입니다. RAW 전체는 두 배치 2,000행입니다. 초기 배치는 RAW에만 유지하며 이번 마트의 대상이 아닙니다.

## WSL 재현·조회

새 환경 또는 이전 단계 환경에서는 관리자 준비 명령으로 Phase 7 DDL을 먼저 적용합니다. 비밀파일과 기존 볼륨은 유지합니다.

```bash
cd "/mnt/c/Users/dwjam/OneDrive/문서/ChatGPT/arirflowETL프로젝트"
python3 scripts/setup_local.py

# DAG 검색·반영 후 여섯 태스크가 있는지 조회
docker compose exec -T airflow airflow tasks list ai_output_quality_pipeline
docker compose exec -T airflow airflow dags list-import-errors

# 입력 JSONL과 manifest가 준비된 상태에서 실제 실행
bash scripts/trigger_pipeline.sh
docker compose exec -T airflow airflow dags list-runs ai_output_quality_pipeline -o json

docker compose exec -T project-postgres psql -X -U etl_user -d etl_project \
  < sql/mart/009_inspect_quality_mart.sql
```

UI `http://localhost:8080` → `ai_output_quality_pipeline` → 해당 run → `build_quality_metrics`/`check_batch_thresholds` → Logs. 게이트 로그에 입력·격리 건수·실제 비율·기준·정책 버전·판정이 있습니다. XCom에는 작은 집계만 저장합니다. 태스크는 `etl_app`으로 실행하며 DDL·비밀 값 출력은 하지 않습니다.

## 남은 확인과 다음 단계

Phase 8에서 임계치 초과·정확한 경계·NULL 차원/날짜 대체·동일 run 재시도·마트 재처리·DB 중단·동시 실행·롤백을 실제 시연합니다. 이번에는 정상 배치의 실제 실행과 DB 조회만 수행했습니다. 별도 자동 테스트나 실패 주입은 실행하지 않았습니다. Phase 7 구현이 완료됐어도 전체 장애 검증까지 완료한 것은 아닙니다.

## 주요 파일과 참고

- `sql/mart/008_create_quality_mart.sql`: 마트·규칙 상세·게이트 이력·현재 뷰.
- `src/mart/quality.py`: 집계·동일성 확인·정책 1.0·판정 커밋.
- `sql/mart/009_inspect_quality_mart.sql`: 차원별/규칙별/게이트 조회.
- `airflow/dags/ai_output_quality_pipeline.py`: 집계·게이트 태스크 추가.
- `src/etl/process_batch.py`: 새 결과 저장과 동시에 이전 마트 무효화.
- [PostgreSQL 16 UNIQUE/NULL 제약 공식 문서](https://www.postgresql.org/docs/16/ddl-constraints.html).
