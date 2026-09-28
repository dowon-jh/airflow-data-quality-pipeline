# Airflow ETL 프로젝트 통합 정리 — Phase 0부터 현재까지

기준일: 2026-09-28  
프로젝트: **AI Output Data Quality Pipeline**  
개발 환경: Windows + Ubuntu WSL 2 + Docker Desktop

## 1. 프로젝트 목적과 현재 위치

후속 재현 확인도 완료했습니다. 같은 PC의 별도 설정·빈 DB/Airflow 볼륨에서 기본 DAG가 성공했고 1,000/959/41/44·마트 5·게이트 PASS를 재현했습니다. 기존 환경은 전후 동일합니다. [빈 환경 확인 보고](CLEAN_INSTALL_VERIFICATION.md).

상품 설명을 OCR·AI로 추출했을 때 발생할 수 있는 누락, 중복, 잘못된 값 등을 가상 데이터로 재현하고, 원본 보존 → 품질 검사 → 정상·오류 분리 → 품질 집계로 처리하는 배치 ETL 프로젝트입니다.

첫 버전은 Python·SQL·PostgreSQL·Airflow·Docker Compose를 사용합니다. 실제 회사 자료와 실제 LLM API는 사용하지 않습니다. 가상 추출 결과의 `model_version`은 비교용 메타데이터이며 실제 모델을 호출한 기록이 아닙니다.

**현재 구현된 흐름**

```text
WSL에서 Python 가상 데이터 생성
    → Airflow: 입력 수집 → PostgreSQL RAW 적재
    → Airflow: Python 규칙 10개로 RAW 품질 검사 (XCom에는 요약만)
    → Airflow: CLEAN·QUARANTINE·이슈 DB 저장 및 성공 처리 이력 보관
    → Airflow: 날짜·카테고리·모델별 품질 마트 → 배치 격리율 게이트
```

개별 수동 명령으로 행별 JSON 품질 보고서도 생성할 수 있습니다. DAG 실행은 보고서 파일을 갱신하지 않습니다.

**최종 사용 절차**

```text
환경 준비 → 입력 생성 → 전체 DAG 실행 → 최신 배치 결과·게이트 조회
```

현재는 **Phase 0~9 기본 버전 완료 상태**입니다. 확정 묶음 13개 실제 DAG run에서 임계치·입력 오류·DB 중단/재시도/복구·재처리를 확인했습니다. 실제 DB 롤백·동시 처리·준비 작업 복구도 검증했습니다. 기존 두 배치와 기본 959/41/44·마트 5그룹·PASS는 그대로이며 실험 데이터 1,300행을 포함한 현재 RAW는 총 3,300행입니다. 실행·복구·증거 보관은 [RUNBOOK](RUNBOOK.md), 기획안 대조와 한계는 [최종 보고](PHASE9_FINAL_REPORT.md)에 정리했습니다. 새 설치의 기본 실행은 RAW 1,000행입니다. 아래 Phase 0~7 수치는 당시 기록입니다.

## 2. 단계별 수행 과정과 의미

| 단계 | 수행한 작업 | 의미·역할 | 현재 상태 |
|---|---|---|---|
| Phase 0 설계 | 범위, 데이터 계층, 규칙, 장애 처리 기준, 로드맵 작성 | 무엇을 만들고 어떤 오류를 어떻게 처리할지 정함 | 작성 완료; 후속 검토에서 정책 충돌 1개 발견 |
| Phase 1 환경 | WSL·Docker Desktop 연동, Airflow·PostgreSQL 기동, 로그인·접속 확인 | 같은 환경에서 프로그램과 DB를 실행할 기반 마련 | 구성 완료 |
| Phase 2 생성기 | 고정 시드의 정상·오류 JSONL, 분리 라벨, 매니페스트 생성 | 실무 데이터 오류를 안전하게 재현할 연습 입력 마련 | 구현 완료; 이후 길이 예시 수정 |
| Phase 3 RAW 적재 | 원본·배치 이력 테이블, 입력 검증, 트랜잭션 적재 | 오류값도 보존하고 배치 단위로 원본 추적 | 구현 완료 |
| Phase 3 후 보안 개선 | 관리자·실행 계정 분리, 암호 교체·파일 주입, DB 호스트 포트 제거 | 작업 코드가 불필요한 관리자 권한과 암호를 가지는 범위 축소 | 주요 개선·추가 DB 권한 제한 적용 |
| Phase 4 품질 검사 | DQ001–DQ010, 정규화, 다중 이슈 기록, JSON 보고서 | 사용 가능한 데이터와 검수가 필요한 데이터를 판정 | 구현·실행 완료; 현재 규칙 1.1 |
| Phase 4 후 재검토·개선 | 경계값·오판 재현, 권한 조회, 코드·문서 대조 및 후속 수정 | 다음 단계에 영향을 줄 문제를 발견하고 개선 | 4개 개선 적용; Phase 8에서 준비 실패 복구 시연 완료 |
| Phase 5 분리 적재 | 최신 결과·전체 이슈·성공 실행 요약 DB 저장 | 정상·오류를 실제 분리하고 재실행 중복 방지 | 구현·적재·같은 UUID 재시도·새 실행 재처리 완료 |
| Phase 6 DAG | 수집·RAW·품질·분리 적재 연결, 재시도·실패 경계 설정 | 실행 순서와 상태를 Airflow에서 관리 | 연결 완료; 현재 여섯 태스크, 실패 시연은 Phase 8 완료 |
| Phase 7 품질 마트·게이트 | 일자·카테고리·모델별 집계, 실행별 판정 | 품질 점수판과 배치 사용 기준 마련 | 4.1% PASS; 초과·경계 시연은 Phase 8 완료 |
| Phase 8 장애·복구 | 임계치·입력 오류·DB 중단·재시도·롤백·동시성·준비 작업 복구 | 실패 경계와 복구 가능성 확인 | 확정 러너·준비 복구 checker PASS |
| Phase 9 최종 문서 | 실행·조회·복구·증거 보관·완료 기준 대조 | 처음 보는 사람이 따라 할 안내와 결과 해석 기준 마련 | 문서 완료; 새 PC 초기 설치 실행은 미검증 |

### Phase 0 — 설계

원본 `RAW`, 정상 결과 `CLEAN`, 오류 보관 `QUARANTINE`, 집계 `MART`의 역할을 나눴습니다. 개별 행의 품질 오류와 DB 접속·스키마·배치 임계치 같은 파이프라인 실패를 구분했습니다.

재실행 시 중복을 피하고, 원본을 보존하며, 이슈를 규칙 ID로 추적하는 방향을 정했습니다. 상세 설계는 아키텍처·데이터 모델·규칙·장애 시나리오·결정 기록에 나눠 작성했습니다.

### Phase 1 — 실행 환경

개발 명령은 Ubuntu WSL의 Bash로 통일했습니다. Docker Desktop의 WSL 연동을 사용하며 Airflow와 프로젝트 PostgreSQL을 별도 컨테이너로 구성했습니다.

| 구성 | 현재 설정 |
|---|---|
| Airflow | `apache/airflow:3.3.2`, `standalone`, `LocalExecutor` |
| 프로젝트 DB | `postgres:16`, DB `etl_project` |
| Airflow UI | `http://localhost:8080`, 호스트 `127.0.0.1` 바인딩 |
| 프로젝트 DB 접속 | Docker 내부 `project-postgres:5432`; 호스트 포트 미게시 |
| 데이터 지속성 | PostgreSQL과 Airflow 각각 named volume 사용 |
| 프로젝트 코드·입력 | Airflow에 읽기 전용 mount |

Airflow 로그인과 컨테이너 간 DB 접속을 확인했습니다. 서비스 재생성 때도 기존 볼륨을 유지했습니다. Airflow 자체 메타데이터와 프로젝트 데이터의 저장 공간은 분리했습니다.

### Phase 2 — 가상 데이터 생성

기본 옵션은 1,000행, seed 42, 전체 오류 주입률 4%, 그 안에 포함된 속성 NULL 주입률 1%, 가상 모델 버전 `v1.0`입니다. 기본 배치는 오류 미주입 960행과 오류 주입 40행으로 구성합니다.

- 입력: 상품 ID·카테고리·원문·OCR·추출 속성·배치 ID·원천 행 ID·생성 시각.
- 라벨: 어느 행에 어떤 오류를 주입했는지 별도 파일로 기록.
- 매니페스트: 생성 옵션, 분포, 배치 ID, 입력 파일 SHA-256.
- 정답 라벨은 RAW 적재와 품질 엔진에 입력하지 않습니다.
- 초기 동일 설정 재생성 파일은 `cmp`로 같음을 확인했습니다.
- 30행 예시는 모든 오류 유형을 살펴보기 위한 샘플이며 오류율 50%입니다.

Phase 4 실행 중 길이 초과 예시가 180자라는 오류를 발견했습니다. 240자로 수정하고 생성기 버전 1.1을 자동 배치 ID 계산·매니페스트에 포함했습니다. 기존 배치는 유지하고 새 배치로 적재했습니다. 생성기 버전과 가상 모델 버전은 별개입니다.

### Phase 3 — RAW 원본 적재

`raw.ai_product_output`에 원본을, `raw.ingest_batch`에 해시·건수·파일명·적재 시각을 저장합니다. RAW 기본 키는 `(batch_id, source_record_id)`입니다. 상품 ID 중복 자체가 검사 대상이므로 `product_id`에는 고유 제약을 두지 않았습니다.

적재기는 JSON 필드·타입·시간대·단일 배치·원천 행 ID 중복·매니페스트를 확인하고 한 트랜잭션으로 저장합니다. 같은 배치 ID와 같은 내용은 추가 적재하지 않고, 같은 ID에 다른 내용은 실패 처리합니다. 같은 배치 동시 적재는 advisory lock으로 직렬화합니다.

초기 1,000행 조회에서 NULL 상품 ID 4개, NULL 속성 10개, 공백 속성 3개, 중복 상품 ID 그룹 4개가 RAW에 보존된 것을 확인했습니다.

### 보안 개선 — Phase 3 이후

처음에는 Airflow가 DB 수퍼유저로 접속했고 DB 암호를 `.env`와 컨테이너 설정 환경변수로 전달했습니다. 검토 후 다음을 적용했습니다.

- 관리자 `etl_user`: 테이블 생성·권한 변경 등 준비 작업 담당.
- 실행 계정 `etl_app`: RAW SELECT·INSERT, 향후 결과 계층 DML 담당. 수퍼유저·DB 생성·역할 생성 권한 없음.
- 적재기에서 DDL을 제거하고 관리자 준비 스크립트로 분리.
- 관리자 암호 교체 및 실행 계정 별도 암호 생성.
- 암호를 OneDrive 밖 `/home/dwjam/.config/ai-output-dq-pipeline/secrets/`에 저장하고 Compose secrets로 연결.
- Airflow에는 실행 계정의 비밀파일만 연결. `.env`에는 일반 설정과 파일 경로만 저장.
- 불필요한 PostgreSQL 호스트 포트 5433 게시 제거.

전환 중 암호 파일 끝 줄바꿈 때문에 접속 실패가 발생했습니다. SQL에서 줄바꿈을 명시적으로 제거하도록 수정하고, 제한 계정으로 기존 배치 재적재가 `already loaded: 1000 rows`로 끝나는 것을 확인했습니다.

### Phase 4 — 품질 엔진

정규화는 Unicode NFKC, 공백 정리, ID·카테고리 대문자 변환입니다. RAW는 변경하지 않습니다. 한 행의 여러 이슈를 모두 기록합니다.

| 규칙 | 검사 내용 | 처리 |
|---|---|---|
| DQ001 | 상품 ID NULL·공백 | ERROR |
| DQ002 | 상품 ID 형식 불일치 | ERROR |
| DQ003 | 정규화 ID의 배치 내 중복 | 그룹 전체 ERROR |
| DQ004 | 허용되지 않은 카테고리 | ERROR |
| DQ005 | 추출 속성 NULL | ERROR |
| DQ006 | 추출 속성 빈 문자열·공백 | ERROR |
| DQ007 | 속성에 글자·숫자가 없음 | ERROR |
| DQ008 | 정규화한 속성이 200자 초과 | ERROR |
| DQ009 | OCR과 원문의 정규화 후 불일치 | WARNING |
| DQ010 | 원문에 같은 숫자·단위 근거가 없는 후보 | ERROR; 환각 확정은 아님 |

ERROR가 하나 이상이면 격리 대상, ERROR가 없으면 통과 대상입니다. DQ009만 있는 행은 통과 대상이며 경고를 별도로 남깁니다. 숫자·단위 비교는 제한된 규칙이고 단위 환산이나 의미 전체 검증은 하지 않습니다. 숫자 파서 결함을 발견한 경위와 후속 개선 상태는 4절에 기록했습니다.

## 3. 현재 데이터와 실제 실행 결과

### Phase 5 적용 기록

기본 배치 `batch-a2ebfbe5e814`를 DB에 분리 적재했습니다. 결과 PK는 배치·원천 행 ID이고 이슈는 규칙 ID까지 포함합니다. CLEAN은 ID·카테고리·속성을 정규화하고 QUARANTINE은 원본 업무 값을 유지합니다. WARNING은 RAW FK로 저장합니다.

배치 최신 결과는 한 트랜잭션으로 교체하고 성공 요약은 `clean.processing_run`에 누적합니다. Phase 5 당시 이력은 2개·최신 표시 1개였습니다. 같은 현재 UUID 재시도는 추가 저장 없이 반환했고 새 UUID 재처리 후에도 결과는 959/41/44로 유지됐습니다. 전체 과거 행 snapshot은 누적하지 않습니다.

DB 조회에서 결과 집합 중복 0개, 격리 업무 값과 RAW의 차이 0개, CLEAN에 연결된 WARNING 3개를 확인했습니다. 초기 배치는 RAW에만 유지하며 이번에 분리 적재하지 않았습니다.

### Phase 6 적용 기록

2026-09-28 15:58 KST에 `manual__2026-09-28T06:58:12.656512+00:00`을 실제 실행했습니다. `collect_input → load_raw → evaluate_quality → load_results` 네 태스크가 모두 성공했습니다. 준비된 기본 입력과 기존 RAW를 재사용했고 DAG 결과 처리 UUID는 `8249849a-e3e5-5948-83e9-0a2732392002`입니다. 실제 run ID를 DB 결과와 이력에 저장했습니다.

결과 959/41/44, WARNING 3개, 결과 중복 0개, 격리 원본 차이 0개, RAW 총 2,000행을 유지했습니다. 성공 요약 이력은 기존 수동 2개와 DAG 1개로 총 3개이며 최신은 1개입니다. 수동 스케줄·catchup 없음·동시 run 1개로 구성합니다. 같은 run은 동일 UUID를 사용하고 새 run은 새 이력을 만듭니다. XCom에는 원본 행 대신 해시·ID·집계만 전달합니다.

### Phase 7 적용 기록

실제 run `manual__2026-09-28T07:16:11.179345+00:00`에서 집계·게이트를 포함한 여섯 태스크가 성공했습니다. UTC 2026-09-01·v1.0의 BAG/LAMP/SHOES/TUMBLER/UNKNOWN 5개 그룹을 저장했습니다. 합계 입력 1,000·통과 959·격리 41·경고 3·이슈 44, NULL 속성 10·중복 ID 8·근거 후보 3행입니다.

정책 1.0은 배치 전체 격리율 > 5%이면 실패입니다. 이번 4.1%는 PASS이며 판정·실제 run ID·처리 UUID를 DB에 저장했습니다. 그룹별/NULL 게이트는 적용하지 않습니다. 생성 날짜 결측은 적재일 UTC를 사용하고 누락 건수를 남깁니다. 새 분리 적재가 이전 마트를 무효화하고 최신 마트는 배치별 교체, 판정 이력은 보존합니다. 현재 성공 처리 이력은 4개·최신 1개, 게이트 이력은 1개입니다. RAW 전체 2,000행을 유지합니다.

### Phase 8 최신 적용

확정 묶음 `20260928T075054`은 13개 DAG run·증거 이벤트 32개를 기록하고 PASS로 완료했습니다. 5%는 통과, 6%·가상 모델 v1.1의 NULL 20%는 게이트 실패로 표시하며 조사 데이터를 보존했습니다. 수정 입력을 새 배치로 처리해 이전 오류 RAW도 유지했습니다.

필수 컬럼 누락·빈 입력·배치 내용 충돌은 해당 단계에서 차단됐습니다. 같은 run clear의 실제 attempt 증가와 처리/판정 이력 불변, 새 run 재처리의 건수 유지·새 이력 1개, superseded run 거부를 확인했습니다. 결과/마트 저장 중간 오류를 주입해 이전 전체 행 스냅샷이 복원됐습니다. 동일 UUID 두 연결은 성공 이력 1개를 만들고 마트를 무효화·재구축했습니다.

DB 일시 중단은 2차 시도로 자동 복구했습니다. 지속 중단은 3회 실패·후속 차단 후 DB를 복구하고 특정 run의 load_raw부터 clear해 4차 성공했습니다. 준비 스크립트도 commit 전 실패와 commit 후 publication 실패의 복구를 확인했습니다. 서비스 running/healthy·비밀파일/설정 내용 불변을 확인했습니다.

### RAW 보존 현황

| 배치 | 역할 | 행 수 |
|---|---|---:|
| `batch-208ca532eb1f` | 초기 생성기 배치; 원본 유지 | 1,000 |
| `batch-a2ebfbe5e814` | 생성기 1.1의 길이 예시 수정 배치 | 1,000 |
| 합계 | 두 배치 보존 | 2,000 |

위 표는 기존 기준 배치입니다. Phase 8 예비 실험 6배치 600행과 확정 실험 7배치 700행을 별도로 추가해 **전체 RAW는 3,300행**입니다. 기존 두 배치의 원본·결과·이력·마트·게이트 행 내용은 전후 SHA-256 비교에서 동일했습니다. 예비 러너는 clear 검증 문제로 중단했으며 해당 재실행 판정은 확정 증거에서 제외했습니다.

### 새 배치의 검사 결과

| 항목 | 결과 |
|---|---:|
| 입력 | 1,000행 |
| 통과 대상 | 959행 |
| 격리 대상 | 41행 |
| 경고가 있는 행 | 3행; 모두 통과 대상에 포함 |
| 전체 이슈 | 44개 |
| 격리 대상 비율 | 4.1% |

규칙별 이슈 수는 DQ001부터 DQ010 순서로 **4, 4, 8, 3, 10, 3, 3, 3, 3, 3개**입니다.

오류 주입 40행과 격리 대상 41행이 다른 이유는 다음과 같습니다. 중복 ID를 복사받은 원래 행 4개도 격리 대상에 포함되고, OCR 경고 3행은 통과 대상으로 처리합니다. 따라서 `40 + 4 - 3 = 41`입니다. 여러 이슈의 개수와 고유 격리 행 수는 구분해야 합니다.

보고서는 `data/generated/quality-report.json`에 저장합니다. 규칙 버전·검사 시각·입력 해시 메타데이터·행 식별자·정규화 값·판정·모든 이슈를 포함합니다. 입력 해시는 적재 이력에서 가져온 값이며 검사 단계에서 DB 내용을 다시 해싱한 값은 아닙니다.

## 4. 오류·장애·보안 검토를 통해 확인한 내용

### 실제 발견·재현·조치 상태

| 사건·문제 | 확인 방법 | 현재 결과 |
|---|---|---|
| 컨테이너 미실행으로 로그인 암호 조회 실패 | 초기 화면에서 `service airflow is not running` 확인 | 이후 서비스 기동·로그인 완료 |
| 암호 파일 줄바꿈으로 DB 인증 실패 | 보안 전환 후 제한 계정 접속 시 실제 오류 발생 | 수정 후 접속·기존 배치 재실행 성공 |
| 길이 초과 예시가 실제 180자 | 초기 배치 검사에 DQ008이 없고 생성 코드 길이 확인 | 240자로 수정, 새 배치에서 DQ008 3건 기록 |
| `1,500ml`를 `500ml`로 읽는 오판 | 메모리 입력으로 양방향 사례 재현; 잘못 PASS | 규칙 1.1에서 수정; 두 사례 모두 DQ010 |
| 준비 실패 시 Airflow 중지·설정 변경 상태가 남음 | 준비 스크립트 명령 순서·예외 경로 검토 | 사전 중지 제거·임시 설정 검증·복구 구현; Phase 8 commit 전후 주입·복구 확인 |
| 같은 배치를 수정한다는 문서와 RAW 불변 정책 충돌 | 장애 문서·적재기·결정 기록 대조 | 새 배치 적재·같은 RAW 재평가 정책으로 정리 |
| 실행 계정의 다른 기본 DB 접속 권한 | 실제 계정으로 CONNECT·TEMP 조회 | 기본 DB CONNECT/TEMP 제거 후 false 확인 |

기능·보안 재검토에서 속성 200자 허용/201자 오류, NULL·공백 분리, OCR 단독 경고 통과, 정규화 ID 중복 그룹 처리를 확인했습니다. 천 단위 쉼표 숫자의 잘못된 통과도 같은 검토에서 재현했습니다.

현재 권한·Docker 설정 조회에서는 다음을 확인했습니다.

- `etl_app` 관리자 관련 권한과 역할 membership 없음.
- RAW 수정·삭제·테이블 생성 불허.
- Docker 설정 환경변수에 암호 값·과거 연결 URI 없음.
- Secret mount 읽기 전용, 비밀 디렉터리 0700.
- Airflow UI loopback 바인딩, PostgreSQL 호스트 포트 없음.
- privileged 모드와 Docker socket mount 없음.
- 기존 RAW 배치와 적재 해시 메타데이터 유지.

추가 검토에서 `postgres` DB CONNECT/TEMP와 `template1` CONNECT가 실행 계정에 남아 있음을 발견했습니다. 후속 개선에서 두 DB의 실행 계정 CONNECT/TEMP가 모두 false임을 확인했습니다.

### 장애 검증의 현재 범위

초기 실행 문제·데이터 결함·경계 오판을 개선한 뒤 Phase 8에서 실제 DAG 실패·재시도·복구·재처리, 품질 임계치 차단, 결과/마트 롤백, 동일 UUID 동시 처리와 준비 작업 복구를 검증했습니다. 주입 방식과 범위는 [Phase 8](PHASE8_FAILURE_RECOVERY.md)에 기록했습니다.

실제 timeout 소진·대용량 부하·DB 강제 종료 crash recovery·복구 명령 자체 실패·이미지 CVE·침투 검증은 수행하지 않았습니다.

## 5. 주요 파일과 역할

| 파일 | 역할 |
|---|---|
| `docker-compose.yml` | 서비스·volume·네트워크·secret 연결 |
| `scripts/setup_local.py` | 관리자 환경 준비와 DB 권한 적용 |
| `src/generator/generate_product_data.py` | 합성 입력·라벨·매니페스트 생성 |
| `src/common/db.py` | 파일 기반 암호로 제한 계정 접속 |
| `src/load/load_raw.py` | 입력 검증·RAW 배치 적재 |
| `sql/raw/002_create_raw_tables.sql` | RAW·배치 적재 이력 DDL |
| `sql/security/004_runtime_role.sql` | 관리자·실행 계정과 권한 분리 |
| `src/dq/engine.py` | 현재 규칙 버전 1.1과 행별 판정 |
| `src/dq/evaluate_raw.py` | 읽기 전용 RAW 조회·품질 보고서 생성 |
| `scripts/evaluate_quality.sh` | WSL 검사 실행·보고서 파일 교체 |
| `scripts/review_current.py` | 비밀 값 출력 없이 상태 조회·DQ 경계 사례 재현 |
| `src/etl/process_batch.py`, `scripts/process_batch.sh` | 제한 계정으로 분리·이슈 적재·최신 결과 교체 |
| `sql/etl/006_create_result_tables.sql` | 결과·이슈·성공 처리 이력 DDL |
| `airflow/dags/ai_output_quality_pipeline.py` | 여섯 태스크 DAG·Params·재시도·timeout |
| `src/etl/pipeline.py`, `scripts/trigger_pipeline.sh` | 메타데이터 계약·기존 함수 연결·WSL trigger |
| `src/mart/quality.py` | 현재 실행 집계·게이트 정책 1.0·판정 커밋 |
| `sql/mart/008_create_quality_mart.sql`, `sql/mart/009_inspect_quality_mart.sql` | 마트·규칙 상세·판정 DDL 및 조회 |
| `scripts/run_phase8.py`, `src/validation/phase8_probe.py` | 실제 DAG·DB 실험·스냅샷·대상 run clear |
| `scripts/check_setup_recovery.py` | 준비 작업 commit 전후 오류·복구 검증 |

`.env`와 `data/generated/`는 Git ignore 대상입니다. Phase 9 당시에는 Git 커밋이 없었고, 이후 [GitHub 공개 저장소](https://github.com/dowon-jh/airflow-data-quality-pipeline)에 코드·문서·검증 요약을 정리합니다. 원본 생성 데이터·암호·실험 작업 폴더는 공개하지 않습니다.

## 6. WSL에서 현재 흐름 실행하기

처음 준비는 `python3 scripts/setup_local.py`로 시작합니다. 상세 최초 설치·로그인·결과 조회는 [RUNBOOK](RUNBOOK.md)에 있습니다. 이미 준비된 현재 환경의 기본 실행은 다음과 같습니다.

```bash
cd "/mnt/c/Users/dwjam/OneDrive/문서/ChatGPT/arirflowETL프로젝트"
docker compose up -d
docker compose ps
python3 src/generator/generate_product_data.py
bash scripts/trigger_pipeline.sh
docker compose exec -T airflow airflow dags list-runs ai_output_quality_pipeline -o json
```

기본 생성기 옵션과 버전이 같으면 같은 배치를 만듭니다. 이미 적재된 같은 내용은 RAW를 늘리지 않습니다. 검사 보고서의 기본 경로는 마지막 실행 결과로 교체됩니다. 배치별 보고서를 보존하려면 출력 경로를 따로 지정합니다.

위 DAG가 수집부터 마트·게이트까지 실행합니다. 실행 중 입력·코드는 변경하지 않습니다. trigger 성공은 실행 접수이므로 UI에서 해당 run의 여섯 태스크 완료를 확인합니다.

현재 DAG는 품질 집계·게이트까지 수행합니다. Phase 7 이전 환경은 먼저 `python3 scripts/setup_local.py`로 마트 DDL을 적용합니다. 마트 조회:

```bash
docker compose exec -T project-postgres psql -X -U etl_user -d etl_project \
  < sql/mart/009_inspect_quality_mart.sql
```

상태·권한 검토:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 scripts/review_current.py
docker compose exec -T project-postgres psql -X -U etl_user -d etl_project \
  < sql/security/005_inspect_permissions.sql
```

새 환경 준비·관리자 마이그레이션 명령은 `python3 scripts/setup_local.py`입니다. 사전 중지 제거·실패 경계 복구를 구현하고 Phase 8에서 commit 전후 복구를 확인했습니다. 일상적인 서비스 시작에는 `docker compose up -d`를 사용합니다. 중지는 `docker compose down`이며 데이터 volume은 유지합니다. `down -v`는 volume 데이터를 삭제합니다.

Airflow 로그인 사용자 이름은 `admin`입니다. 암호 값은 이 문서에 기록하지 않습니다. 로컬에서 확인할 때는:

```bash
docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

## 7. 최종 완료 상태와 이후 범위

| 순서 | 작업 | 완료 기준 |
|---|---|---|
| 우선 개선 — 적용 완료 | F-01 숫자 파서, F-02 준비 실패 경계, F-03 재처리 정책, S-01 DB 접근 범위 | 숫자 오판·권한 수정과 Phase 8 준비 실패 복구 확인 |
| Phase 5 — 구현·실행 완료 | CLEAN·QUARANTINE·이슈 DB 적재 | 기본 배치 분리, WARNING 보존, 재시도·재처리 건수 유지 확인 |
| Phase 6 — 완료 | Airflow DAG 연결 | 현재 여섯 태스크·실제 run ID·DB 결과·Phase 8 실패/재시도 확인 |
| Phase 7 — 완료 | 품질 마트·배치 임계치 | 5그룹 집계·4.1% PASS·Phase 8 품질 저하 차단 확인 |
| Phase 8 — 수행 완료 | 장애·재실행·복구 시연 | 확정 DAG/DB 실험과 준비 복구 checker PASS |
| Phase 9 — 완료 | 실행·설계·장애 기록 마무리 | README·RUNBOOK·최종 보고 작성; 독립 새 PC 실행은 미검증 |

같은 RAW는 같은 배치로 재실행·재평가하며 수정된 입력은 새 배치로 적재합니다. 결과·마트는 최신 결과를 유지하고 처리 요약·규칙 버전·게이트 판정은 이력으로 남깁니다. 수정 배치 연결 메타데이터는 미구현입니다. 빈 배치는 거부하며 5%/6% 게이트 경계와 초과 실패를 시연했습니다. 실무 확장은 기본 프로젝트 완성 후 검토합니다.

## 8. 프로젝트를 설명하는 짧은 문장

> 가상 상품 추출 데이터를 Airflow의 수집·RAW·검사·분리 적재·집계·게이트로 연결했다. 기본 959/41/44와 품질 마트를 확인했고, 별도 입력에서 임계치 초과·입력 오류·DB 중단·재시도/복구·동시 처리·롤백과 준비 작업 복구를 시연했다. 원본과 조사 데이터를 보존하고 WSL 실행·조회·복구 절차까지 문서화한 기본 학습 프로젝트다.

## 상세 근거 문서

- [빈 환경 재현 확인](CLEAN_INSTALL_VERIFICATION.md)
- [실행·재현 안내서](RUNBOOK.md), [Phase 9 최종 보고](PHASE9_FINAL_REPORT.md)
- [Phase 8 장애·재실행·복구](PHASE8_FAILURE_RECOVERY.md)
- [Phase 7 품질 마트와 게이트](PHASE7_QUALITY_MART.md)
- [Phase 6 Airflow DAG와 실행 결과](PHASE6_AIRFLOW_DAG.md)
- [Phase 5 분리 적재와 실행 결과](PHASE5_SPLIT_LOAD.md)

- [기획안 대비 수행 검토](PLAN_ALIGNMENT_REVIEW.md)

- [진행 기록](PROGRESS.md)
- [초기 설계](../AIRFLOW_PROJECT_BRIEF.md), [전체 로드맵](../ROADMAP.md)
- [아키텍처](ARCHITECTURE.md), [데이터 모델](DATA_MODEL.md), [결정 기록](DECISIONS.md)
- [가상 데이터](SYNTHETIC_DATA.md), [규칙 정의](DATA_QUALITY_RULES.md), [Phase 4 결과](PHASE4_QUALITY_ENGINE.md)
- [초기 보안 검토](SECURITY_REVIEW.md), [보안 개선](LOCAL_SECURITY_SETUP.md)
- [후속 기능·보안 검토](PHASE4_REVIEW.md), [장애 시나리오 설계](FAILURE_SCENARIOS.md)
