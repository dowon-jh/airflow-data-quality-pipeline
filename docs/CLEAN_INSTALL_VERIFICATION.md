# 분리된 빈 환경의 재현 확인

수행일: 2026-09-28, 18:17 KST. **결과: PASS.** 같은 PC의 Ubuntu WSL과 Docker Desktop에서 기존 프로젝트와 별도의 초기 환경을 만들어 기본 실행을 재현했습니다.

## 1. 환경 분리

| 항목 | 기존 환경 | 이번 검증 환경 |
|---|---|---|
| Compose 이름 | `ai-output-dq-pipeline` | `ai-output-dq-clean-20260928t091708-656fae` |
| UI 포트 | 8080 | 8081 (loopback) |
| DB / Airflow 볼륨 | 기존 named volume | 새 이름의 두 named volume |
| 작업 폴더 | 원래 프로젝트 폴더 | `data/generated/clean-install/20260928T091708-656fae/workspace/` |
| DB 비밀파일 | WSL 기본 환경 경로 | `/home/dwjam/.config/ai-output-dq-clean-20260928t091708-656fae/secrets/` |

코드·SQL·DAG·문서를 복사했습니다. 기존 `.env`, 생성 데이터, DB/Airflow 볼륨, 비밀파일은 복사하지 않았습니다. 시작 전에 새 볼륨 두 개와 비밀파일 경로가 없음을 확인했습니다. setup 후 데이터 테이블 9개가 모두 0행이었습니다.

분리를 위해 `PROJECT_COMPOSE_NAME`과 `PROJECT_AIRFLOW_PORT`를 추가했습니다. 기본값은 기존 이름과 8080입니다. setup이 해당 이름의 볼륨·서비스를 확인하고 `~/.config/<환경 이름>/secrets/`를 사용하며 새 `.env`에 설정을 저장합니다. 기존 환경 설정은 그대로입니다.

## 2. 실제 순서와 결과

`scripts/check_clean_install.py`가 별도 작업 폴더에서 문서 순서대로 수행했습니다.

1. setup으로 DB 초기화·DDL·실행 계정 권한 적용·Airflow 기동.
2. 데이터 테이블 9개 모두 0행 확인.
3. DAG 등록·import 오류 없음·로그인 암호 파일 생성 확인.
4. 기본 생성기로 1,000행 생성, trigger 스크립트로 전체 DAG 실행.
5. run·태스크 상태·DB 건수·게이트·실행 계정 권한 조회.
6. 검증용 서비스 중지, 기존 환경의 전후 해시·컨테이너 상태 비교.

실제 run ID: `manual__2026-09-28T09:17:36.017849+00:00`. 여섯 태스크와 DAG 모두 `success`였습니다.

| 확인 항목 | 실제 결과 |
|---|---:|
| RAW 배치 / 행 | 1 / 1,000 |
| CLEAN / QUARANTINE | 959 / 41 |
| WARNING 행 / 이슈 | 3 / 44 |
| 성공 처리 이력 / 최신 마트 / 규칙 상세 | 1 / 5 / 20 |
| 게이트 이력 / 격리율 / 판정 | 1 / 4.1% / PASS |
| 규칙 / 게이트 정책 | 1.1 / 1.0 |
| DAG import 오류 | 0 |
| `etl_app` 수퍼유저·DB/역할 생성·복제·RLS 우회 | 모두 false |

## 3. 기존 환경 보존과 증거

기존 DB 9개 테이블의 전체 행을 정렬 직렬화한 SHA-256·행 수, 기존 `.env`·DB 비밀파일 해시, 컨테이너 ID·실행·health 상태가 모두 전후 동일했습니다. 기존 RAW 3,300행과 새 환경 RAW 1,000행은 별개입니다. 비밀번호 값은 증거에 기록하지 않았습니다.

- 로컬 `data/generated/clean-install/20260928T091708-656fae/evidence.json`: 빈 환경·빈 DB·태스크·건수·판정·기존 환경 전후 비교.
- 같은 로컬 폴더의 `setup.log`, `trigger.log`: 준비·실행 접수 기록.
- GitHub 공개용 [검증 증거 요약](evidence/verification-summary.json): 로컬 증거에서 추출한 run 상태·건수·판정·기존 환경 보존 결과. 비밀파일 해시·개인 경로·컨테이너 ID는 제외했습니다.

증거·복사 작업 폴더는 generated Git ignore 대상이므로 별도로 보관합니다. 검증용 컨테이너는 **중지 상태**이며 결과 볼륨과 비밀파일은 보존했습니다. 현재 8081 UI는 중지되어 있습니다.

검증 결과를 다시 열려면 WSL에서:

```bash
cd "/mnt/c/Users/dwjam/OneDrive/문서/ChatGPT/arirflowETL프로젝트/data/generated/clean-install/20260928T091708-656fae/workspace"
docker compose up -d
docker compose ps
docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
# 브라우저: http://localhost:8081, 사용자 admin
```

종료는 이 폴더에서 `docker compose stop`으로 합니다. 새 `.env`에 검증용 이름과 포트가 저장되어 있습니다.

## 4. 다시 검증하기

기존 기본 환경이 실행 중이고 8081 포트가 비어 있을 때 원래 프로젝트 루트의 WSL에서:

```bash
python3 -u scripts/check_clean_install.py
```

매번 고유한 이름·작업 폴더·빈 볼륨·비밀파일 경로를 만듭니다. 이번 evidence 경로가 출력됩니다. 성공하면 검증용 서비스만 중지하고 결과를 보존합니다. 오류로 중단되면 evidence의 `workspace`를 확인하고 해당 폴더에서 `docker compose stop`으로 검증 서비스를 중지할 수 있습니다. 볼륨을 자동 삭제하지 않습니다.

## 5. 결론과 확인 범위

기존 테이블·데이터·Airflow 이력 없이 setup으로 DB 객체와 권한을 만들고, 생성 → 수집 → RAW → 검사 → 분리 → 마트 → 게이트의 기본 흐름을 재현했습니다. 기대 결과와 일치합니다.

**같은 PC의 빈 애플리케이션 환경 검증입니다.** Docker Desktop·Ubuntu WSL·Python과 이미 내려받은 이미지는 재사용했습니다. 별도 PC/새 OS, 이미지 최초 다운로드, 브라우저 로그인 입력, DB 백업 복원, 장애 시연 전체 재실행은 이번 확인에 포함하지 않았습니다. 로그인 암호 파일 생성만 확인했습니다.
