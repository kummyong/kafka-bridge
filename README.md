# Kafka Bridge

카프카 클러스터 A(source)의 특정 토픽 메시지를 수신해서 카프카 클러스터 B(target)의 토픽으로 그대로 전달하는 브릿지 프로그램. 브로커 주소, 토픽, SASL/SSL 설정을 웹 화면에서 입력하고 시작/중지할 수 있다.

## 사전 준비

- Node.js 18 이상 (개발은 22 버전 기준)
- 소스/타겟 카프카 클러스터에 네트워크로 접근 가능해야 함 (브로커의 `advertised.listeners`가 이 서버에서 실제로 접근 가능한 주소를 광고하는지 확인)
- 네이티브 모듈이 없는 순수 JS 의존성(kafkajs, express)이라 별도 빌드 도구(build-essential 등) 불필요

## 설치

```bash
git clone https://github.com/kummyong/kafka-bridge.git
cd kafka-bridge
npm install
```

## 실행

```bash
npm start
```

브라우저에서 `http://localhost:3000` 접속.

장시간 운영할 경우 터미널을 닫아도 죽지 않도록 `pm2`나 `systemd` 서비스로 데몬화 권장:

```bash
# pm2 예시
npm install -g pm2
pm2 start server.js --name kafka-bridge
pm2 save
```

## 테스트

실제 카프카 클러스터 없이 `kafkajs`를 목(mock) 처리해서 동작을 검증한다. `bridge.js`(브릿지 로직)와 `app.js`(Express API)를 대상으로 하며, 커버리지 임계값(문/브랜치/함수/라인 90% 이상)을 만족하지 못하면 `test:coverage`가 실패한다.

```bash
npm test              # 전체 테스트 실행
npm run test:coverage # 커버리지 리포트 포함 (임계값 미달 시 실패)
```

## 웹 화면에서 설정하기

1. **Source Cluster (A)**: 브로커 주소(콤마로 구분), 토픽, 컨슈머 그룹 ID, 필요 시 SSL/SASL(PLAIN, SCRAM-SHA-256/512) 입력
2. **Target Cluster (B)**: 브로커 주소, 토픽, 필요 시 SSL/SASL 입력
3. **설정 저장** 클릭 → 로컬 `config.json`에 저장됨 (git에는 포함되지 않음, `.gitignore` 처리됨)
4. **시작** 클릭 → 브릿지가 A 토픽을 구독해서 메시지를 그대로 B 토픽으로 전달 시작
5. 화면에서 상태(running/stopped/error), 처리 건수(consumed/produced/errors), 최근 메시지 20건, 로그를 3초 주기로 확인 가능
6. **중지** 클릭으로 종료

## API

| Method | Path | 설명 |
|---|---|---|
| GET | `/api/config` | 저장된 설정 조회 |
| POST | `/api/config` | 설정 저장 (source/target 필수) |
| POST | `/api/start` | 브릿지 시작 |
| POST | `/api/stop` | 브릿지 중지 |
| GET | `/api/status` | 상태 및 통계 조회 |
| GET | `/api/logs` | 최근 로그 조회 |
| GET | `/api/messages` | 최근 처리 메시지(최대 20건) 조회 |

## 부하 테스트용 스크립트

`continuous-producer.js`는 소스 클러스터(`localhost:9092`, 토픽 `orders`)로 초당 10건씩 랜덤 메시지를 계속 보내는 테스트용 스크립트. 브로커 주소/토픽은 파일 상단에서 직접 수정.

```bash
node continuous-producer.js
```

## 알아두어야 할 것

- **메시지 처리 방식**: 페이로드를 역직렬화하지 않고 key/value/headers를 바이트 그대로 전달(pass-through). Java 직렬화, Spring Kafka `JsonSerializer`(`__TypeId__` 헤더 포함) 등 대부분의 포맷은 그대로 통과한다. 단, Avro/Protobuf + Schema Registry를 쓰고 A/B가 서로 다른 레지스트리를 쓰는 경우에는 스키마 ID가 안 맞을 수 있어 별도 변환 로직이 필요하다.
- **처리량**: 현재는 메시지 1건마다 `producer.send`를 기다리는 구조라 초당 수십~백 건대 트래픽에는 무리 없지만, 순간적으로 트래픽이 크게 몰리는 경우(예: 초당 수천 건)에는 `eachBatch` 기반 배치 처리로 전환하는 것을 고려해야 한다.
- **보안**: `config.json`에 SASL 비밀번호가 평문으로 저장된다. 운영 환경에 배포할 경우 `chmod 600 config.json`으로 권한을 제한하고, 필요하면 별도 시크릿 관리로 전환할 것.
- **오프셋 커밋**: kafkajs 기본 auto-commit 정책을 따르며, 타겟으로의 전송이 성공한 뒤에만 다음 메시지로 넘어가므로 at-least-once로 동작한다(전송 중 프로세스가 죽으면 재시작 시 일부 메시지가 중복 전달될 수 있음).
