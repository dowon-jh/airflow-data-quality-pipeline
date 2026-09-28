# Phase 0–4 기능·보안 재검토

검토일: 2026-09-28. 범위: 현재 코드·문서, 실제 Docker 설정, PostgreSQL 권한·RAW 상태, DQ 경계 사례. 개인 PC의 WSL 로컬 학습 환경을 기준으로 합니다.

## 결론

**후속 개선 적용:** 같은 날짜에 F-01 숫자 검사, F-02 실패 경계, F-03 재처리 문서, S-01 DB 접근 제한을 개선했습니다. 아래는 개선 전 검토 기록입니다. 실제 변경·재실행 결과와 실패 복구 미시연 범위는 [개선 기록](PHASE4_REMEDIATION.md)에 있습니다.

현재 기본 합성 배치의 생성 → RAW 적재 → 품질 보고서 흐름은 연결돼 있습니다. 기존 수퍼유저 사용·암호 환경변수 전달·DB 호스트 포트 게시 문제는 개선돼 있습니다. 확인한 범위에서 즉시 외부 침해로 이어지는 중대 보안 결함은 발견하지 못했습니다.

다만 기능 문제 2개, 문서 정책 충돌 1개, DB 접근 범위 개선 사항 1개를 확인했습니다. **숫자 파서와 설정 실패 복구는 Phase 5 전에 수정하는 것을 권장합니다.** 이번 검토에서는 애플리케이션·DB 권한을 수정하지 않았고, 재현 스크립트와 검토 기록을 추가했습니다.

## 발견 사항

### F-01 — 숫자·단위 파서가 큰 숫자의 일부만 읽어 오류를 놓침 — 중간

- 위치: `src/dq/engine.py:11`, `src/dq/engine.py:27`, `src/dq/engine.py:85`.
- 실제 재현: 원문 `용량 500ml`, 속성 `1,500ml`을 검사하면 속성 토큰을 `500ml`로 읽습니다. DQ010이 발생하지 않고 PASS입니다.
- 반대 방향도 재현: 원문 `용량 1,500ml`, 속성 `500ml` 역시 PASS입니다.
- 원인: 숫자 앞 경계에서 쉼표를 막지 않아 `findall`이 쉼표 다음부터 부분 일치합니다. 문서상 천 단위 표현이 범위 밖이어도, 코드가 이를 정상 숫자로 잘라 해석하는 것은 오판을 일으킵니다.
- 영향: 현재 기본 생성기는 쉼표 숫자를 만들지 않아 기록된 959/41 분류에는 직접 영향이 없습니다. 실제 형태의 상품 데이터로 넓히면 잘못된 속성이 CLEAN 대상으로 갈 수 있습니다.
- 권장 수정: 숫자 전체 토큰을 해석하고 천 단위 쉼표를 지원하거나, 미지원 숫자 형식을 명시적으로 표시해 부분 일치가 정상 근거로 쓰이지 않도록 합니다. 음수·부호·한글 접미사 경계도 함께 정리합니다. 규칙 버전을 갱신합니다.

### F-02 — 관리자 준비 실패 시 설정과 서비스 상태가 복구되지 않음 — 중간

- 위치: `scripts/setup_local.py:45`, `scripts/setup_local.py:48`, `scripts/setup_local.py:54`.
- 코드에서 확인: `.env`를 먼저 교체하고 Airflow를 중지한 뒤 PostgreSQL 시작·DDL·권한 SQL을 수행합니다. 이후 명령이 실패하면 예외로 종료하며 `.env` 복구나 Airflow 재시작을 수행하지 않습니다.
- 영향: 정상 실행 중이던 환경에서 DB 시작이나 마이그레이션이 실패하면 Airflow가 중지된 채 남습니다. 오류 출력을 모두 숨겨 원인 파악도 어렵습니다. 암호 파일이 유실돼 새 파일이 생성된 뒤 SQL 적용이 실패한 경우 파일과 기존 DB 암호가 일시적으로 불일치할 수 있습니다.
- SQL 트랜잭션은 DB 변경을 롤백하지만 호스트 파일과 컨테이너 중지 상태까지 롤백하지는 않습니다.
- 권장 수정: 설정을 임시 파일로 준비하고 사전 검증, DB 적용, 설정 확정, 서비스 시작의 실패 경계를 설계합니다. 실패 시 이전 설정·서비스 상태를 복구하거나 복구 명령을 명확히 제공하고, 암호를 가린 오류 정보를 남깁니다.
- 근거 수준: 실제 서비스 장애를 만들지는 않았습니다. 현재 코드의 명령 순서와 예외 경로를 검토한 결과입니다.

### F-03 — “같은 배치 오류를 수정 후 재실행”과 RAW 불변 정책이 충돌 — 중간, Phase 5 설계 항목

- 위치: `docs/FAILURE_SCENARIOS.md:22`.
- 해당 문서는 같은 배치의 오류를 수정한 뒤 재실행할 수 있어야 한다고 설명합니다. 반면 `src/load/load_raw.py`와 D-011은 같은 배치 ID에 다른 해시가 들어오면 실패하도록 구현했습니다.
- 영향: Phase 5 재처리에서 RAW를 덮어쓰는 구현으로 이어질 수 있습니다. 현재 실행 버그는 아니지만 정책 충돌입니다.
- 권장 정리: 같은 RAW·같은 규칙으로의 재실행, 같은 RAW·새 규칙 버전으로의 재평가, 수정된 입력을 새 배치 ID로 적재하는 경우를 구분합니다. 원본은 유지하고 이전 배치와의 연결 정보를 따로 보관합니다.

### S-01 — 실행 계정이 프로젝트 외 기본 DB에도 접속 가능 — 낮음

- 위치: `sql/security/004_runtime_role.sql:14`.
- 실제 실행 계정 조회: `etl_project`는 CONNECT=true/TEMP=false, `postgres`는 CONNECT=true/TEMP=true, `template1`은 CONNECT=true/TEMP=false입니다.
- 원인: PUBLIC 권한 제한이 `etl_project`에만 적용돼 다른 DB의 기본 권한이 남습니다. PostgreSQL은 기본적으로 DB CONNECT·TEMPORARY 권한을 PUBLIC에 부여합니다. [PostgreSQL 16 권한 문서](https://www.postgresql.org/docs/16/ddl-priv.html)
- 영향: 수퍼유저 권한 획득을 의미하지는 않습니다. 다만 Airflow 작업이 필요 없는 DB에 접속하거나 `postgres`에 임시 테이블을 만들어 자원을 사용할 수 있습니다.
- 권장 수정: 이 로컬 클러스터의 다른 사용자를 확인한 뒤 불필요한 DB의 PUBLIC CONNECT/TEMP를 제한합니다. PostgreSQL에는 권한 DENY가 없어 실행 계정에서 직접 REVOKE만 해서는 PUBLIC 상속을 없앨 수 없습니다.

## 정상 동작과 보호 장치

실제 설정과 읽기 전용 DB 조회로 확인했습니다.

- `etl_app`: 수퍼유저·DB 생성·역할 생성·복제·RLS 우회 권한 없음. 상속된 역할 membership도 없음.
- RAW SELECT·INSERT 허용, UPDATE·DELETE·schema CREATE·프로젝트 DB CREATE/TEMP 불허.
- Docker 설정 환경변수에 암호 값 및 과거 `AIRFLOW_CONN_PROJECT_POSTGRES` 없음. 비밀파일 경로만 있음.
- Airflow에는 실행 계정 비밀파일만 연결. 관리자 암호 파일은 PostgreSQL에만 연결. 실제 secret mount는 모두 읽기 전용.
- WSL 비밀 디렉터리 권한 0700. 파일의 0444는 컨테이너 UID가 읽도록 한 설정이며 호스트 일반 사용자 접근은 상위 디렉터리에서 제한.
- Airflow UI는 `127.0.0.1:8080`만 게시. PostgreSQL은 호스트 포트 미게시.
- 두 서비스 모두 privileged=false, Docker socket 미마운트.
- `.env`와 생성 결과 보고서가 Git ignore 대상.
- RAW는 이전·신규 배치 각 1,000행, 총 2,000행. 이전 배치 메타데이터 해시 유지.
- 적재 SQL 매개변수화, 동일 배치 advisory lock, 한 트랜잭션 적재, DQ 읽기 전용 snapshot은 서로 충돌하지 않음.
- PASS 행의 경고를 보존하도록 향후 이슈 테이블 FK를 RAW로 설계한 것은 DQ009 정책과 맞음.
- 40개 주입 행과 41개 격리 대상의 차이는 중복 원래 행 4개 포함 및 경고 3개 통과 정책으로 설명됨. 이슈 44개도 집계 방식과 일치.

## 경계 사례 실행 결과

검토용 입력은 메모리에서만 만들고 DB에 적재하지 않았습니다.

| 사례 | 실제 결과 | 판단 |
|---|---|---|
| 속성 200자 | PASS | 길이 경계 정책과 일치 |
| 속성 201자 | DQ008 / QUARANTINE | 정책과 일치 |
| 속성 NULL | DQ005만 발생 | 정책과 일치 |
| 속성 공백 | DQ006만 발생 | 정책과 일치 |
| OCR만 불일치 | DQ009 / PASS | WARNING 정책과 일치 |
| ID `P000001`과 ` p000001 ` | 두 행 모두 DQ003 | 정규화·그룹 정책과 일치 |
| 원문 500ml / 속성 1,500ml | PASS | F-01 오판 재현 |
| 원문 1,500ml / 속성 500ml | PASS | F-01 오판 재현 |

## 현재 구성의 한계

- Compose secrets는 파일 mount 방식입니다. Docker 관리자·WSL root·해당 실행 계정 코드는 비밀을 읽을 수 있습니다. 암호화 저장소 또는 악성 DAG 격리는 제공하지 않습니다. [Docker secrets 안내](https://docs.docker.com/compose/how-tos/use-secrets/)
- Simple Auth Manager는 개발·테스트용이며 LocalExecutor는 신뢰하는 DAG 작성자를 전제로 합니다. 현재 localhost 학습 범위에서 사용하고, 타 사용자·외부 공개 시 인증과 실행 격리를 별도로 설계해야 합니다. [Airflow 보안 모델](https://airflow.apache.org/docs/apache-airflow/3.3.2/security/security_model.html)
- 품질 엔진은 전체 배치를 메모리에 올리므로 대량 입력의 메모리 제한은 없습니다. 현재 합성 1,000행 범위의 제약입니다.
- 기본 출력 파일은 마지막 실행 보고서로 교체됩니다. 여러 배치 동시 실행·장기 이력은 별도 출력 경로나 Phase 5 DB 결과 저장으로 처리해야 합니다.
- 이미지/의존성의 CVE 스캔, 실제 외부 침투 시험, OneDrive 과거 동기화 이력 확인은 이번 검토 범위에 포함하지 않았습니다. 현재 Git에는 커밋이 없어 복구 기준점도 아직 없습니다.

## 재현 명령

Ubuntu WSL 프로젝트 루트에서:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 scripts/review_current.py
docker compose exec -T project-postgres psql -X -U etl_user -d etl_project \
  < sql/security/005_inspect_permissions.sql
```

검토 스크립트는 DB·컨테이너·암호·기존 보고서를 변경하지 않습니다. 암호 값이나 전체 Docker inspect 결과를 출력하지 않고 필요한 상태만 요약합니다.
