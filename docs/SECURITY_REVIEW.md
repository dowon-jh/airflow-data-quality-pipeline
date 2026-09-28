# Phase 3 보안 검토

검토일: 2026-09-26  
범위: 로컬 Docker Compose 설정, WSL 작업 디렉터리, DB 역할, RAW 적재 코드와 비밀정보 관리. 현재 구성은 개인 학습용 로컬 환경입니다.

## 결론

**2026-09-28 후속 조치:** 아래 1~3번 발견 사항에 대한 개선을 적용했습니다. Airflow는 제한 계정 `etl_app`을 사용하며, 관리자 암호를 교체하고 두 계정의 암호를 OneDrive 밖 WSL 홈에서 Compose secrets로 전달합니다. 호스트 포트 5433도 제거했습니다. 아래 본문은 2026-09-26 검토 당시의 기록입니다. 현재 실행 방법과 남는 제약은 [로컬 보안 설정](LOCAL_SECURITY_SETUP.md)을 참고하세요.

현재 포트는 `127.0.0.1`에만 연결되고, 실제 DB 암호는 Git에서 제외되어 있습니다. RAW 적재 SQL은 정적 DDL과 매개변수화된 값 적재를 사용합니다. 외부 네트워크에 열린 서비스나 코드에 하드코딩된 실제 암호는 확인되지 않았습니다.

다만 **Airflow가 PostgreSQL 수퍼유저로 접속하는 구성**과 **DB 암호를 컨테이너 환경변수로 전달하는 구성**은 다음 단계에서 DAG 실행 범위가 커지기 전에 개선할 가치가 있습니다.

## 발견 사항

### 1. Airflow의 DB 계정에 수퍼유저 권한이 있음 — 우선순위 높음

`docker-compose.yml`은 `POSTGRES_USER=${PROJECT_DB_USER}`로 PostgreSQL을 초기화하고 같은 계정으로 `AIRFLOW_CONN_PROJECT_POSTGRES`를 구성합니다. 실행 중인 DB에서 `etl_user`의 `rolsuper`, `rolcreaterole`, `rolcreatedb`가 모두 `true`임을 확인했습니다. PostgreSQL 공식 이미지도 `POSTGRES_USER`가 수퍼유저를 생성한다고 설명합니다. [PostgreSQL 공식 이미지 문서](https://hub.docker.com/_/postgres)

따라서 앞으로 Airflow 태스크 코드나 의존성이 오작동하면 프로젝트 DB 안에서 필요한 범위를 넘는 관리자 권한으로 동작할 수 있습니다. 현재는 개인 PC의 로컬 컨테이너에 한정된 위험입니다.

**개선안:** 초기화·스키마 변경용 관리자 계정과 ETL 실행용 제한 계정을 분리합니다. Airflow에는 제한 계정만 연결하고, 필요한 schema/table의 `USAGE`, `SELECT`, `INSERT`, `UPDATE` 등만 부여합니다. DDL 수행은 별도 관리자 작업으로 분리합니다. 기존 볼륨의 역할과 소유권을 고려해 마이그레이션합니다.

### 2. DB 암호가 환경변수와 OneDrive 작업 폴더에 있음 — 우선순위 중간

`.env`의 `PROJECT_DB_PASSWORD`가 Compose 보간을 통해 PostgreSQL의 `POSTGRES_PASSWORD`와 Airflow의 연결 URI 환경변수에 들어갑니다. Docker 문서는 환경변수의 의도치 않은 노출 위험을 설명하고 Compose secrets를 권장합니다. [Docker Compose secrets 안내](https://docs.docker.com/compose/how-tos/use-secrets/)

`.env`는 Git에서 무시되며, 현재 값은 예시 암호가 아닌 48자리 16진수 값입니다. 프로젝트가 OneDrive 경로 아래에 있으므로 `.env`가 동기화될 수 있습니다. 실제 OneDrive 동기화 상태와 공유 대상은 이 검토에서 확인하지 않았습니다. 컨테이너 환경변수는 Docker 접근 권한이 있는 로컬 사용자에게 노출될 수 있습니다.

**개선안:** 비밀정보를 동기화 폴더 밖에 두고 Compose secrets 또는 파일 기반 비밀 주입으로 전환합니다. PostgreSQL 이미지는 `POSTGRES_PASSWORD_FILE` 방식을 지원합니다. Airflow 연결 암호를 환경변수에 넣지 않는 방식도 함께 설계해야 합니다. 전환 후 기존 DB 암호의 교체 여부를 결정합니다.

### 3. PostgreSQL이 호스트 포트 5433을 사용함 — 우선순위 낮음

`docker-compose.yml`은 `127.0.0.1:5433:5432`로 게시합니다. 외부 네트워크에는 직접 열리지 않지만, 현재 작업은 `docker compose exec`와 컨테이너 내부 연결만 사용하므로 호스트 포트가 필수는 아닙니다.

**개선안:** WSL/호스트의 SQL 클라이언트 연결이 필요하지 않으면 PostgreSQL `ports` 항목을 제거합니다. Airflow UI의 `127.0.0.1:8080` 바인딩은 로컬 사용을 유지합니다.

### 4. Airflow 인증 방식은 로컬 개발 범위에 적합함 — 배포 시 재검토

현재 `standalone` 실행과 기본 Simple Auth Manager는 개발·테스트용입니다. Apache Airflow는 Simple Auth Manager를 운영 환경 인증 수단으로 사용하지 말라고 명시합니다. 현재 UI는 `127.0.0.1`에만 바인딩돼 있으며 관리자 비밀번호는 자동 생성 파일에 있습니다. [Airflow 보안 모델](https://airflow.apache.org/docs/apache-airflow/3.3.2/security/security_model.html), [Airflow 설치 안내](https://airflow.apache.org/docs/apache-airflow/stable/installation.html)

이 프로젝트를 다른 PC나 인터넷에 공개할 때는 인증, TLS, 네트워크 제한을 별도로 설계해야 합니다. 현재 로컬 학습 단계에서 즉시 변경할 항목은 아닙니다.

## 확인된 보호 장치

- Airflow와 PostgreSQL의 호스트 포트는 모두 `127.0.0.1`에만 바인딩돼 있습니다.
- 실제 `.env`는 `.gitignore`에 포함돼 있고, Git 원격도 설정돼 있지 않습니다.
- 프로젝트의 `dags`, `src`, `sql`, `data`는 Airflow 컨테이너에 읽기 전용으로 마운트됩니다.
- RAW 적재는 데이터 값을 `psycopg2` 매개변수와 `execute_values`로 전달하며, SQL 문자열에 레코드 값을 직접 이어 붙이지 않습니다.
- 입력은 가상 데이터이며 실제 회사 데이터나 내부 SOP를 포함하지 않습니다.

## 다음 작업 전 권장 순서

1. 관리자 계정과 Airflow 실행 계정을 분리합니다.
2. 암호 저장 위치와 컨테이너 주입 방식을 정리합니다.
3. 호스트에서 PostgreSQL에 직접 접속할 계획이 없다면 포트 5433 게시를 제거합니다.
4. 변경 후 Airflow의 DB 접속과 RAW 적재 경로를 확인합니다.

이 검토는 현재 로컬 구성과 코드에 한정됩니다. 외부 공개, 타 사용자 접근, 실제 데이터 취급을 시작하면 별도 배포 보안 검토가 필요합니다.
