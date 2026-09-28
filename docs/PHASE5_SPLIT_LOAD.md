# Phase 5 — CLEAN·QUARANTINE 분리 적재

구현·실행일: 2026-09-28. 적용 규칙 버전: 1.1.

## 구현 내용

RAW 배치를 읽어 기존 품질 엔진으로 판정하고 정상 행·오류 행·이슈를 DB에 저장합니다. JSON 보고서를 DB 입력으로 읽지 않고 동일한 RAW와 규칙을 사용해 직접 처리합니다. 라벨 파일도 사용하지 않습니다.

| 테이블 | 역할 |
|---|---|
| `clean.ai_product_output` | 최신 PASS 행. 상품 ID·카테고리·추출 속성 정규화 |
| `quarantine.ai_product_output` | 최신 ERROR 행. 업무 필드와 원천 적재 시각은 RAW 그대로 |
| `quarantine.dq_issue` | 최신 행별 모든 ERROR·WARNING. 한 행의 여러 규칙 저장 |
| `clean.processing_run` | 성공 실행별 배치·규칙 버전·UUID·DAG ID·건수·규칙 집계·최신 여부 |

결과 두 테이블의 PK는 `(batch_id, source_record_id)`, 이슈 PK는 `(batch_id, source_record_id, rule_id)`입니다. CLEAN에는 배치 내 정규화 상품 ID 고유 제약도 있습니다. 원본 참조 FK와 처리 실행 참조 FK를 둡니다. DQ009 WARNING만 있는 행은 CLEAN에 있으며 이슈 테이블은 RAW를 참조하므로 경고도 저장할 수 있습니다.

`processing_run_id`는 수동 실행에도 부여하는 UUID입니다. `dag_run_id`는 수동 실행에서는 NULL이며 Phase 6에서 실제 Airflow 실행 ID를 전달합니다. 결과와 이슈에 규칙 버전·실행 ID·처리 시각·근거 검사 상태도 남깁니다.

## 재실행·이력 정책

- 배치별 최신 결과만 보관합니다. 새 처리 실행은 해당 배치 결과·이슈를 삭제하고 다시 적재합니다.
- 성공한 실행의 요약 이력은 누적합니다. 이전 실행은 `is_current=false`, 최신 실행만 true입니다. 부분 고유 인덱스로 배치당 최신 실행을 한 개로 제한합니다.
- 이력은 요약 이력입니다. 과거 실행별 전체 행·이슈 snapshot을 모두 보존하는 구현은 아닙니다. RAW와 규칙 버전 정보를 함께 추적합니다.
- 실행 UUID를 생략하면 매번 새 UUID를 생성합니다. 결과 건수는 유지되고 성공 이력은 한 개씩 늘어납니다.
- 동일 UUID로 현재 실행을 재시도하면 입력 해시·규칙 버전·DAG ID와 현재 결과를 확인해 `already_processed`로 반환합니다. 이력도 추가하지 않습니다.
- 다른 배치·다른 규칙·다른 DAG ID에 사용한 UUID나 이미 이전 이력으로 바뀐 UUID는 재사용을 거부합니다. 새 실행 UUID를 사용해야 합니다.
- 수정된 입력은 새 배치로 적재하고 이전 RAW를 유지합니다. 수정 배치 간 연결 메타데이터는 아직 구현하지 않았습니다.

## 트랜잭션과 권한

1. 제한 계정 `etl_app`으로 접속합니다.
2. RAW 적재기와 같은 배치 advisory lock을 획득합니다.
3. 배치 적재 이력과 RAW 건수를 확인하고 모든 행을 평가합니다.
4. 최신 실행 표시 변경, 성공 이력 추가, 기존 결과 삭제, 분리·이슈 적재를 한 트랜잭션으로 수행합니다.
5. 결과 건수·규칙별 이슈 수·집합 중복·ERROR 분류·실행 ID 일치를 확인하고 커밋합니다.

오류가 나면 같은 트랜잭션의 결과 삭제·적재·이력 변경은 롤백됩니다. 실패 실행은 성공 이력 테이블에 남기지 않으며 CLI 오류로 기록합니다. advisory lock은 이를 사용하는 코드끼리 동시 실행을 조정합니다. 관리자가 직접 잠금 없이 데이터를 변경하는 작업까지 막는 장치는 아닙니다. [PostgreSQL advisory lock 문서](https://www.postgresql.org/docs/16/explicit-locking.html)

분리 적재기는 기본 READ COMMITTED를 사용해 잠금 대기 후 앞선 RAW 적재의 커밋을 읽습니다. 결과 교체는 한 트랜잭션이지만, 외부 조회자가 여러 SELECT를 각각 수행하면 커밋 전후가 섞일 수 있으므로 일관된 여러 테이블 조회에는 snapshot 트랜잭션이나 한 SQL을 사용합니다. [PostgreSQL 트랜잭션 격리 문서](https://www.postgresql.org/docs/16/transaction-iso.html)

테이블 생성은 관리자 `scripts/setup_local.py`에서 수행합니다. 실행 계정은 RAW SELECT·INSERT 및 결과 계층 DML만 유지하며 DDL 권한을 추가하지 않았습니다.

## 코드와 WSL 실행

| 파일 | 역할 |
|---|---|
| `sql/etl/006_create_result_tables.sql` | 결과·이슈·성공 이력 DDL |
| `src/dq/raw.py` | Phase 4·5가 공유하는 RAW 읽기·집계 |
| `src/etl/process_batch.py` | 판정·분리·트랜잭션 교체·현재 결과 확인 |
| `scripts/process_batch.sh` | WSL에서 기본 매니페스트 배치 또는 지정 배치 실행 |
| `sql/etl/007_inspect_results.sql` | 결과·경고·중복·격리 원본·RAW 건수 조회 |

처음 결과 테이블을 준비할 때:

```bash
cd "/mnt/c/Users/dwjam/OneDrive/문서/ChatGPT/arirflowETL프로젝트"
python3 scripts/setup_local.py
```

준비된 환경의 일상 실행:

```bash
docker compose up -d
bash scripts/process_batch.sh
docker compose exec -T project-postgres psql -X -U etl_user -d etl_project \
  < sql/etl/007_inspect_results.sql
```

인자를 생략하면 생성 매니페스트의 배치 ID를 사용합니다. 특정 배치의 새 실행:

```bash
bash scripts/process_batch.sh batch-a2ebfbe5e814
```

동일 실행의 재시도는 앞선 출력의 `processing_run_id`를 전달합니다. 아래 ID는 문서 작성 당시 최신 성공 실행입니다. 이후 새 실행이 생기면 당시 최신 ID로 바꿔야 합니다.

```bash
bash scripts/process_batch.sh batch-a2ebfbe5e814 \
  --processing-run-id 064b93ca-73e4-47e7-8496-870769bde0aa
```

## 실제 실행 결과

처리 배치: `batch-a2ebfbe5e814` 1,000행. 초기 배치 `batch-208ca532eb1f`는 RAW에만 보존하며 이번 작업에서 분리 적재하지 않았습니다.

| 항목 | 조회 결과 |
|---|---:|
| CLEAN | 959행 |
| QUARANTINE | 41행 |
| 이슈 | 44개: ERROR 41, WARNING 3 |
| CLEAN 행에 연결된 WARNING | 3개 |
| CLEAN/QUARANTINE 중복 행 | 0 |
| QUARANTINE 업무 값·원천 적재 시각과 RAW의 차이 | 0 |
| RAW 전체 | 2,000행 유지 |
| 성공 처리 이력 | 2개; 최신 표시 1개 |

실행 순서는 첫 실행 → 같은 UUID 재시도 → 새 UUID 배치 재처리였습니다. 같은 UUID는 `already_processed`로 반환했고 새 UUID 재처리 뒤에도 959/41/44가 유지됐습니다. 삭제·재적재 방식의 현재 결과와 실행 요약 이력 누적을 구분해 확인했습니다.

## 검증 범위와 남은 작업

이번에는 실제 배치 적재·재실행 및 읽기 전용 SQL 조회를 수행했습니다. 별도 자동 테스트 묶음이나 실제 동시 실행·DB 중단·롤백 장애 주입은 수행하지 않았습니다. 다중 이슈는 코드와 이슈 PK로 지원하며 별도 합성 조합 시연은 후속 검증에 남습니다.

현재 구현은 전체 배치를 메모리에 올리는 로컬 규모용입니다. 빈 배치는 오류로 거부합니다. 품질 마트·임계치 게이트는 아직 없고 Airflow DAG도 다음 단계입니다.

**다음: Phase 6 — 기존 생성·적재·분리 로직을 Airflow DAG로 연결하고 태스크 의존성·실행 ID·재시도·로그를 구성합니다.**
