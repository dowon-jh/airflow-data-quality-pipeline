# Phase 4 검토 후 우선 개선 기록

적용일: 2026-09-28. 대상: [기능·보안 검토](PHASE4_REVIEW.md)의 F-01, F-02, F-03, S-01.

후속 상태: [Phase 8](PHASE8_FAILURE_RECOVERY.md)에서 F-02 준비 작업의 commit 전 실패 복원·commit 후 publication 실패의 forward recovery를 실제 시연하고 PASS를 기록했습니다. 아래 미수행 문장은 당시 기록입니다.

## 변경과 실제 적용 결과

| 항목 | 변경 | 확인 결과 |
|---|---|---|
| F-01 숫자 부분 일치 | 숫자 전체 토큰 검사·천 단위 쉼표 처리, 규칙 버전 1.1 | `500ml`/`1,500ml` 양방향 사례 모두 DQ010으로 격리 대상 |
| F-02 준비 실패 복구 | Airflow 사전 중지 제거, 임시 설정 검증, SQL 커밋 후 설정 확정, 실패 단계별 복구 안내 | 현재 환경에서 정상 적용 성공; 실패 복구는 코드로 구현했고 실제 장애 주입은 미수행 |
| F-03 재처리 정책 | 동일 RAW 재실행·규칙 변경·수정 입력의 새 배치 적재 구분 | 장애 문서와 RAW 불변 정책의 충돌 정리 |
| S-01 다른 DB 접근 | PUBLIC CONNECT/TEMP 제한, 프로젝트 DB에 필요한 계정만 CONNECT | `postgres`, `template1`에서 실행 계정 CONNECT=false/TEMP=false |

기존 비밀 파일·RAW·볼륨을 유지했습니다. 새 데이터 배치를 추가하지 않았습니다. 두 RAW 배치 각 1,000행, 총 2,000행입니다. 기본 배치 검사 보고서는 규칙 1.1로 갱신했고 통과 959행·격리 41행·경고 3행·이슈 44개가 유지됐습니다.

## 1. 숫자 검사

- 지원 단위는 `ml`, `l`, `mm`, `cm`, `w`, `g`, `kg`입니다. 부호·소수·올바른 천 단위 쉼표를 지원합니다.
- `1,500ml`는 Decimal `1500` + 단위 `ml`로 해석합니다. `500.0ml`와 `500ml`는 같습니다. 단위 환산은 하지 않습니다.
- `1,50ml`나 지수 표기처럼 숫자 후보 전체를 지원 문법으로 해석할 수 없으면 부분 숫자를 근거로 쓰지 않고 DQ010 `ATTRIBUTE_EVIDENCE_UNPARSEABLE` ERROR를 기록합니다.
- 한 행·규칙 ID마다 이슈는 하나만 기록합니다. 해석 불가와 근거 부족이 동시에 있으면 해석 불가 사유로 기록합니다.
- 보고서의 `evidence_status`는 `CHECKED`, `UNPARSEABLE`, `NO_SUPPORTED_QUANTITY`입니다. 지원 토큰이 없는 속성은 의미 검증을 수행한 것이 아닙니다.
- 숫자 앞 단어·잘못된 부호가 붙은 표현은 지원 토큰으로 잘라 읽지 않습니다. 예를 들어 `x-500ml`는 `500ml`로 오인하지 않고 `NO_SUPPORTED_QUANTITY`입니다. 다른 ERROR가 없으면 PASS일 수 있으므로 PASS는 구현한 규칙 범위의 통과를 뜻합니다.
- 단위 뒤 한글 접미사는 허용하며 영문·숫자 접미사로 이어지는 토큰은 제외합니다.

## 2. 준비 작업의 실패 경계

`scripts/setup_local.py`의 순서는 사전 확인 → 임시 일반 설정 검증 → PostgreSQL 기동 → 관리자 SQL 트랜잭션 → `.env` 원자적 교체 → Airflow 기동입니다.

1. Docker·DB volume 상태, 기존 설정·비밀 파일, SQL 파일을 먼저 확인합니다. 기존 DB volume이 있는데 비밀 파일이 없으면 암호를 새로 생성하지 않고 복원을 요구합니다.
2. `.env.setup.*`에 비밀 없는 일반 설정만 준비합니다. 파일은 Git에서 제외됩니다.
3. Airflow를 미리 중지하지 않습니다. 정상 환경에서는 불필요한 중단을 피합니다. PostgreSQL 컨테이너가 재생성되는 구성 변경은 여전히 잠깐의 DB 중단을 만들 수 있습니다.
4. SQL 커밋 이전에 실패하면 이전 `.env`를 유지하고 이전 서비스 실행 상태를 복원하려 시도합니다. 복원 자체가 실패하면 원인과 재개 명령을 출력합니다.
5. SQL 커밋 이후에는 오래된 설정으로 되돌리지 않습니다. 현재 비밀 파일을 유지하며 준비 스크립트를 재실행해 완료하도록 안내합니다. 설정 교체 완료 후 Airflow 기동이 실패하면 `docker compose up -d airflow`를 안내합니다.
6. 오류에서 실제 암호와 PASSWORD 리터럴을 가려 진단 정보를 남깁니다.

파일·DB·Docker 상태를 하나의 분산 트랜잭션으로 묶은 구현은 아닙니다. 복구 실패나 Docker 장애 시 재개가 필요할 수 있습니다. 실제 서비스 실패 주입과 복구 시연은 Phase 8에서 수행합니다.

## 3. 재처리 기준

| 상황 | 정책 |
|---|---|
| 같은 입력·같은 규칙 | 같은 배치 재실행, 결과 중복 방지 |
| 같은 RAW·새 규칙 | 같은 배치 재평가, 새 규칙 버전·실행 식별자 기록 |
| 입력 내용 수정 | 새 배치 ID, 이전 RAW 보존 |

Phase 5에서는 결과 적재의 멱등성을 구현합니다. 이전 배치와 수정 배치의 연결 메타데이터 및 규칙 버전별 결과 이력 보관 방식은 결과 테이블 설계에서 확정합니다. 같은 배치 ID의 RAW 내용을 바꾸는 기능은 만들지 않습니다.

## 4. DB 접근 제한

`etl_project`의 PUBLIC CONNECT·CREATE·TEMP를 제거하고 `etl_user`와 `etl_app`에 CONNECT를 부여했습니다. `postgres`, `template1`은 PUBLIC CONNECT·TEMP를 제거했습니다.

이 정책은 해당 로컬 클러스터가 프로젝트 전용이라는 전제입니다. 관리자 SQL은 예상하지 못한 로그인 역할이 있으면 권한 변경 전에 실패해 다른 사용자의 접근을 임의로 차단하지 않습니다. 현재 실행 계정은 관리자 관련 권한·상속 역할이 없고 RAW SELECT·INSERT 권한을 유지합니다.

## WSL 명령

```bash
cd "/mnt/c/Users/dwjam/OneDrive/문서/ChatGPT/arirflowETL프로젝트"
python3 scripts/setup_local.py
bash scripts/evaluate_quality.sh
PYTHONDONTWRITEBYTECODE=1 python3 scripts/review_current.py
```

이번 적용에서 위 순서로 실행했습니다. 검토 스크립트는 DB·서비스·비밀 값을 변경하지 않으며 기존 경계 사례를 메모리에서 재현합니다.

## 다음 단계

Phase 5 CLEAN·QUARANTINE ETL: 통과/오류 레코드의 DB 분리 적재, 경고·다중 이슈 저장, 재실행 중복 방지. Airflow DAG 자동화와 전체 장애 복구 시연은 후속 단계입니다.
