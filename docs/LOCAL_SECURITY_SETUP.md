# 로컬 보안 개선과 실행 방법

적용일: 2026-09-28. Ubuntu WSL에서 실행합니다.

## 무엇을 변경했나

- `etl_user`: DB 초기화·테이블 생성용 관리자. Airflow에는 이 계정의 암호를 전달하지 않습니다.
- `etl_app`: Airflow 작업용 계정. 수퍼유저·DB 생성·역할 생성·복제·RLS 우회 권한이 없습니다.
- RAW는 조회·추가만 허용합니다. 수정·삭제·테이블 생성 권한은 주지 않습니다.
- `clean`, `quarantine`, `mart`는 조회·추가·수정·삭제를 허용하며 테이블 생성은 관리자가 수행합니다. 관리자 `etl_user`가 이후 만드는 테이블에도 기본 권한을 적용했습니다.
- 적재기에서 DDL을 제거했습니다. 같은 배치의 동시 실행은 트랜잭션 advisory lock으로 직렬화합니다.
- 기존 관리자 암호를 새 임의 암호로 교체하고 실행 계정에 별도 암호를 생성했습니다.
- `.env`에는 계정명·DB명·비밀파일 디렉터리 경로만 있습니다. 암호와 연결 URI는 컨테이너 설정 환경변수에서 제거했습니다.
- PostgreSQL 호스트 포트 5433 게시를 제거했습니다. Airflow는 Docker 내부 네트워크의 `project-postgres:5432`로 접속합니다. UI는 `127.0.0.1:8080`입니다.
- 기존 DB 및 Airflow 볼륨을 유지했습니다. Airflow 로그인 계정·암호는 기존 볼륨에 보존됩니다.

## 암호 저장 위치

WSL: `/home/dwjam/.config/ai-output-dq-pipeline/secrets/`

`admin_password`는 PostgreSQL에만, `app_password`는 PostgreSQL과 Airflow에 연결합니다. PostgreSQL이 실행 계정의 암호를 설정해야 하므로 두 파일이 필요합니다. 컨테이너에서는 `/run/secrets/project_admin_password`, `/run/secrets/project_app_password`로 읽습니다.

호스트의 `secrets` 디렉터리는 0700입니다. 내부 파일은 컨테이너의 서로 다른 UID가 읽을 수 있게 0444이며, 다른 일반 WSL 사용자는 상위 디렉터리를 통과할 수 없습니다. Docker 관리자·WSL root는 읽을 수 있습니다. Compose secrets는 암호화된 비밀 저장소가 아닙니다. [Docker 공식 안내](https://docs.docker.com/compose/how-tos/use-secrets/)

이전 `.env`가 OneDrive의 과거 버전에 남아 있을 수 있지만, 해당 DB 암호는 이번 작업에서 교체했습니다. 실제 동기화 이력은 조사하지 않았습니다.

## 새 환경 준비 / 관리자 마이그레이션

```bash
cd "/mnt/c/Users/dwjam/OneDrive/문서/ChatGPT/arirflowETL프로젝트"
python3 scripts/setup_local.py
```

이 스크립트는 새 DB 환경에서 비밀 파일을 생성합니다. 기존 DB volume이 있는데 파일이 없으면 새 암호를 만들지 않고 복원을 요구합니다. 임시 일반 설정을 검증하고 PostgreSQL 관리자 SQL을 적용한 뒤 `.env`를 확정하고 Airflow를 시작합니다. Airflow를 미리 중지하지 않습니다. 기존 비밀 파일과 데이터 볼륨을 유지해 재실행할 수 있습니다. 로컬 DB명 `etl_project`, 관리자명 `etl_user`, 실행 계정명 `etl_app`을 전제로 합니다. 실패 단계별 복구와 재개 방법은 [후속 개선 기록](PHASE4_REMEDIATION.md)에 있습니다.

PostgreSQL 이미지의 초기화 환경변수는 기존 볼륨의 암호를 자동으로 변경하지 않으므로, 스크립트가 SQL로 암호 변경까지 수행합니다. [PostgreSQL 공식 이미지 안내](https://hub.docker.com/_/postgres)

이후 schema/table SQL이 추가되면 이 스크립트의 관리자 SQL 목록에 추가합니다. 일상적인 시작은 아래 명령으로 충분합니다.

```bash
docker compose up -d
docker compose ps
docker compose exec -T airflow python /opt/airflow/project/src/load/load_raw.py \
  --input /opt/airflow/project/data/generated/products.jsonl
docker compose exec -T project-postgres psql -U etl_user -d etl_project \
  < sql/security/005_inspect_permissions.sql
```

호스트 SQL 도구는 현재 5433으로 직접 접속할 수 없습니다. 조회는 위 `docker compose exec`를 사용합니다. Airflow는 로컬 학습용 standalone/Simple Auth 구성입니다.

## 적용 후 상태 조회 결과

| 항목 | 결과 |
|---|---|
| `etl_app` 수퍼유저·DB 생성·역할 생성·복제·RLS 우회 | 모두 false |
| RAW SELECT / INSERT | true / true |
| RAW UPDATE / DELETE | false / false |
| RAW schema CREATE / DB CREATE / TEMP | 모두 false |
| 기존 RAW 데이터 | 1,000행 유지 |
| 기존 배치 SHA-256 | 변경 없음 |
| 제한 계정으로 적재기 재실행 | `already loaded: 1000 rows` |
| Airflow / PostgreSQL | 실행 중 / healthy |
| WSL 비밀파일 디렉터리 권한 | 0700 |

적용 중 PostgreSQL의 기본 `trim`이 파일 끝 줄바꿈을 제거하지 않아 접속 실패가 발생했습니다. SQL에서 공백·탭·줄바꿈을 명시적으로 제거하도록 수정하고 다시 적용한 뒤 정상 접속했습니다.

## 최종 상태

Phase 0~9 기본 버전을 완료했습니다. Phase 8에서 준비 작업 commit 전후 복구도 확인했습니다. 제한 DB 계정·비밀파일·read-only mount를 유지하며 비밀파일 내용은 전후 동일합니다. 처음 준비와 이전 환경 업데이트는 `python3 scripts/setup_local.py`로 수행합니다. [최종 실행 안내서](RUNBOOK.md), [Phase 8 검증 범위](PHASE8_FAILURE_RECOVERY.md).
