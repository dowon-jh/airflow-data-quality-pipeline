# Phase 4 — 규칙 기반 품질 검사

구현일: 2026-09-28. 규칙 버전: 1.0.

후속 상태: 현재 규칙은 숫자 파서를 개선한 **1.1**입니다. 기존 기본 배치의 통과 959행·격리 41행·경고 3행은 유지됩니다. 아래는 최초 구현 기록이며 최신 실패 경계와 파서 정책은 [후속 개선 기록](PHASE4_REMEDIATION.md)에 있습니다.

## 이번 단계의 역할

RAW 원본을 읽어 행마다 PASS 또는 QUARANTINE 대상을 판정합니다. 규칙 ID·필드·ERROR/WARNING·사유를 모두 보고서에 남깁니다. 결과를 CLEAN·QUARANTINE 테이블에 저장하는 작업과 Airflow DAG 자동 실행은 다음 단계입니다.

| 파일 | 역할 |
|---|---|
| `src/dq/engine.py` | DB와 분리된 10개 규칙 및 정규화 |
| `src/dq/evaluate_raw.py` | 제한 계정으로 RAW 조회, 배치 건수 확인, JSON 보고서 출력 |
| `scripts/evaluate_quality.sh` | WSL 실행, 완료된 보고서만 파일로 교체, 요약 출력 |
| `data/generated/quality-report.json` | 행별 검사 결과와 규칙별 집계; Git 제외 |

DB 조회는 읽기 전용 REPEATABLE READ 트랜잭션을 사용합니다. 배치가 없거나 적재 이력의 건수와 RAW 건수가 다르면 중단합니다. 값은 SQL 매개변수로 전달합니다. 라벨 파일은 검사 입력에 사용하지 않습니다. 모든 행을 메모리에 읽으므로 현재 1,000행 규모에 맞춘 구현이며 큰 규모는 스트리밍/SQL 집계를 별도로 설계해야 합니다.

## 확정한 처리 정책

- 중복 ID 그룹 전체를 격리 대상으로 표시합니다. 대표 행을 임의로 선정하지 않습니다.
- DQ009 OCR 불일치는 WARNING입니다. 이것만 있으면 PASS입니다.
- 나머지 규칙은 ERROR이며 하나 이상이면 QUARANTINE입니다.
- 한 행의 여러 이슈를 모두 기록합니다. 이슈 수와 문제 행 수는 다를 수 있습니다.
- 정규화한 ID·카테고리·속성도 보고서에 포함하되 RAW는 변경하지 않습니다.
- 숫자·단위 근거 후보는 규칙으로만 표시하며 환각을 확정하지 않습니다.

세부 조건과 경계값은 [규칙 문서](DATA_QUALITY_RULES.md)에 있습니다. 자동 테스트는 이번 작업에서 추가하거나 실행하지 않았습니다. 아래 결과는 실제 애플리케이션을 배치에 실행한 결과입니다.

## WSL 실행

```bash
cd "/mnt/c/Users/dwjam/OneDrive/문서/ChatGPT/arirflowETL프로젝트"
docker compose up -d
bash scripts/evaluate_quality.sh
```

인자를 생략하면 현재 `data/generated/products.manifest.json`의 배치 ID를 읽습니다. 원본 데이터나 정답 라벨을 검사에 사용하지 않고, 해당 ID로 DB의 RAW를 조회합니다. 다른 배치를 검사하거나 보고서를 따로 보존하려면:

```bash
bash scripts/evaluate_quality.sh batch-a2ebfbe5e814 data/generated/quality-report.json
bash scripts/evaluate_quality.sh batch-208ca532eb1f data/generated/quality-report-original.json
```

보고서의 `summary`는 집계, `records`는 `batch_id`, `source_record_id`, `decision`, `normalized`, `issues`입니다. 원본은 행 식별자로 RAW에서 조회합니다. 실행 실패 시 기존 완료 보고서는 유지되고 임시 파일을 제거합니다. 검사 시각은 매번 바뀌지만 같은 입력·규칙 버전의 판정은 같습니다.

## 실제 실행 결과

생성기 1.1의 기본 배치 `batch-a2ebfbe5e814`:

| 항목 | 건수 |
|---|---:|
| 입력 | 1,000 |
| PASS 대상 | 959 |
| QUARANTINE 대상 | 41 |
| WARNING이 있는 행 | 3 |
| 전체 이슈 | 44 |
| 격리 대상 비율 | 4.1% |

| 규칙 | 이슈 수 |
|---|---:|
| DQ001 | 4 |
| DQ002 | 4 |
| DQ003 | 8 |
| DQ004 | 3 |
| DQ005 | 10 |
| DQ006 | 3 |
| DQ007 | 3 |
| DQ008 | 3 |
| DQ009 | 3 |
| DQ010 | 3 |

주입 오류는 40행이지만 중복으로 복사된 원래 행 4개도 대상에 포함되고, OCR 경고 3행은 통과 대상이므로 격리 대상은 41행입니다. WARNING 3행은 이 배치에서 모두 PASS에 포함됩니다. 이 수치는 검사 대상 분류이며 아직 CLEAN·QUARANTINE DB 적재 건수가 아닙니다. 배치 임계치 실패 처리도 아직 구현하지 않았습니다.

## 발견한 생성기 오류와 수정

기존 생성기의 `EXCESSIVE_LENGTH`는 `길이초과` 45회 반복으로 실제 180자였습니다. 기존 RAW 배치 검사에서는 DQ008이 나오지 않았으며 PASS 962, 격리 대상 38, 경고 3, 이슈 41이었습니다.

생성기를 60회 반복(240자)으로 수정했습니다. 생성기 버전 1.1을 매니페스트와 자동 배치 ID 계산에 포함해 이전과 다른 배치로 적재했습니다. `model_version`은 가상 추출 모델 버전이며 생성기 버전과 별개입니다. 저장소의 30행 예시도 같은 수정으로 갱신했습니다.

기존 배치 `batch-208ca532eb1f` 1,000행과 파일 해시는 DB에 그대로 보존했습니다. 새 배치도 1,000행이므로 현재 RAW 총 행 수는 2,000입니다. 생성기 버전을 바꿀 때 수동 `--batch-id`로 이전 ID를 재사용하면 RAW 적재기는 내용 충돌로 중단합니다.

## 다음 단계

Phase 5: PASS 결과를 CLEAN에, ERROR가 있는 행을 원본과 함께 QUARANTINE에 저장합니다. WARNING도 추적할 수 있게 이슈 테이블은 RAW 행을 참조하도록 설계합니다. 배치 재실행 시 결과를 중복 없이 갱신하는 트랜잭션 처리를 추가합니다.
