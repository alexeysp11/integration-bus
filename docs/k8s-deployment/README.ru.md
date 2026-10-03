# ☸️ Kubernetes Deployment: `integration-bus` через Helm (Kind/K3s)

[English](README.md) | [Русский](README.ru.md)

Этот документ описывает перевод полного стека `docker-compose.yml` (18 workload'ов: 6 .NET-сервисов, Kafka,
Postgres, Redis, Kafka Connect/Debezium, ClickHouse, Metabase, Prometheus, Grafana, Loki, Jaeger, Kafka UI,
Redis Commander) в единый Helm-чарт, разворачиваемый одной командой `helm install` в локальный Kubernetes-кластер
(проверено на **Kind**). Это закрывает Stage 5 [`docs/roadmap.md`](../roadmap.md) из более широкого бэклога проекта.

> 🧑‍🎓 **Никогда не работали с Kubernetes?** Начните с [`GETTING-STARTED.ru.md`](GETTING-STARTED.ru.md) — это
> пошаговая инструкция «что куда жать и скачивать» для полных новичков. Этот документ (ниже) — технический разбор
> архитектуры и найденных проблем для тех, кто уже развернул стек и хочет понять, как он устроен.

---

## 1. Расположение артефактов

```text
deploy/k8s/
├── kind/
│   └── kind-cluster.yaml          # конфигурация локального Kind-кластера с проброшенными портами
└── charts/
    └── integration-bus/
        ├── Chart.yaml
        ├── values.yaml              # все образы, пароли, nodePort'ы в одном месте
        ├── files/                   # копии infrastructure/*.sql, *.json, *.yml, *.sh (см. §5)
        └── templates/
            ├── secrets.yaml
            ├── configmaps.yaml
            ├── postgres.yaml        # StatefulSet + PVC
            ├── redis.yaml           # StatefulSet + PVC, + Redis Commander
            ├── kafka.yaml           # StatefulSet + PVC, headless Service, kafka-provisioner Job, Kafka UI
            ├── kafka-connect.yaml   # Deployment + debezium-registrar Job
            ├── clickhouse.yaml      # StatefulSet + PVC
            ├── metabase.yaml        # Deployment + metabase-driver-setup Job + PVC
            ├── prometheus.yaml
            ├── grafana.yaml
            ├── jaeger.yaml
            ├── loki.yaml            # StatefulSet + PVC
            └── app-services.yaml    # все 6 .NET-сервисов одним DRY-шаблоном (range по values.services)
```

Dockerfile'ы сервисов **не менялись** — у всех шести уже были корректные multi-stage сборки, пригодные для любого
контейнерного рантайма. Единственная инфраструктурная работа — описать топологию в Kubernetes-манифестах.

---

## 2. Быстрый старт

```bash
# 1. Поднять локальный кластер (один раз)
kind create cluster --config deploy/k8s/kind/kind-cluster.yaml

# 2. Собрать образы сервисов (как для docker-compose) и загрузить кастомные образы в кластер
docker compose build
for img in integration-bus-integration-bus-gateway-api \
           integration-bus-integration-bus-processing-api \
           integration-bus-integration-bus-saga-orchestrator-service \
           integration-bus-integration-bus-account-balance-service \
           integration-bus-integration-bus-compliance-service \
           integration-bus-integration-bus-core-ledger-service \
           integration-bus-integration-bus-kafka-connect; do
  kind load docker-image "$img:latest" --name integration-bus
done

# 3. Развернуть весь стек одной командой
kubectl create namespace integration-bus
helm install integration-bus deploy/k8s/charts/integration-bus -n integration-bus --timeout 10m
```

`helm install` сам:
1. Поднимает Postgres/Redis/Kafka/ClickHouse (как `StatefulSet` с `PersistentVolumeClaim`) и все 6 .NET-сервисов.
2. Запускает post-install `Job`-хуки по очереди: `kafka-provisioner` (создаёт все Kafka-топики) →
   `metabase-driver-setup` (докачивает ClickHouse-драйвер) → `debezium-registrar` (регистрирует 3 Debezium source +
   3 ClickHouse sink коннектора).

Публичные образы (`postgres`, `redis`, `apache/kafka` и т.д.) **не обязательно** грузить через `kind load` — нода
Kind имеет доступ в интернет и подтянет их напрямую из реестра при первом поступлении соответствующих подов.
`kind load docker-image` для них в этой среде **не работает** из-за известной несовместимости с containerd-backed
image store Docker Desktop (см. §4.4) — для наших 7 самосборных образов (единственных, у которых нет реестра) этот
способ работает штатно.

### Доступ к сервисам

Если кластер создан через `deploy/k8s/kind/kind-cluster.yaml`, порты проброшены на `localhost` с конвенцией
**host-порт = docker-compose-порт + 10000** (чтобы Kind мог работать одновременно с уже поднятым docker-compose
стеком на той же машине, без конфликтов):

| Сервис | docker-compose | Kind (localhost) |
|---|---|---|
| Gateway.Api | 5038 | **15038** |
| Processing.Api | 5201 | **15201** |
| SagaOrchestrator /health | 6001 | **16001** |
| AccountBalance /health | 6002 | **16002** |
| Compliance /health | 6003 | **16003** |
| CoreLedger /health | 6004 | **16004** |
| Grafana | 3000 | **13000** |
| Jaeger UI | 16686 | **26686** |
| Prometheus | 9090 | **19090** |
| Kafka UI | 8080 | **18080** |
| Redis Commander | 8082 | **18082** |
| Metabase | 3001 | **13001** |
| Kafka Connect REST | 8083 | **18083** |

---

## 3. Перенос топологии docker-compose → Kubernetes

Ключевой приём, упростивший перенос: **имена Kubernetes `Service` совпадают 1:1 с именами контейнеров
docker-compose** (`integration-bus-kafka`, `integration-bus-db`, `integration-bus-redis`, `integration-bus-loki`,
`integration-bus-jaeger` и т.д.). Благодаря этому **ни одна переменная окружения сервисов не изменилась** —
`Kafka__BootstrapServers=integration-bus-kafka:9094`, `ConnectionStrings__SagaDb=Host=integration-bus-db;...` и
весь Serilog/OTel-блок перекочевали из `docker-compose.yml` в `values.yaml` буквально без правок.

Остальные типовые преобразования:
- Однократные "init"-контейнеры compose (`kafka-provisioner`, `debezium-registrar`, `metabase-driver-setup`) стали
  Helm post-install `Job`-хуками (`helm.sh/hook: post-install,post-upgrade`), с `hook-weight` для правильного
  порядка: сначала топики, затем регистрация коннекторов.
- `depends_on: condition: service_healthy` заменён на `initContainers` с простым `nc -z <service> <port>` —
  ожидание доступности TCP-порта перед стартом основного контейнера.
- Stateful-сервисы (Postgres, Redis, Kafka, ClickHouse, Loki) стали `StatefulSet` с `volumeClaimTemplates` —
  Kind поставляет дефолтный `StorageClass` (`rancher.io/local-path`) из коробки, PVC биндятся автоматически.
- 6 однотипных .NET-сервисов не продублированы вручную 6 раз, а описаны одним шаблоном `app-services.yaml` с
  `range $name, $svc := .Values.services` по карте в `values.yaml` — добавить седьмой сервис означает дописать
  один блок в `values.yaml`, а не копировать 70 строк YAML.

---

## 4. Troubleshooting: типовые симптомы при развёртывании в Kubernetes

Ниже — диагностический справочник по нетривиальным сценариям сбоя, которые актуальны именно при переносе этого
стека в Kubernetes (а не в docker-compose). Конфигурация чарта уже учитывает все перечисленные фиксы; раздел
полезен, если вы меняете шаблоны чарта или разворачиваете похожую топологию самостоятельно.

### 4.1. Самоподключение Kafka через обычный ClusterIP — дедлок

**Симптом:** под `integration-bus-kafka-0` бесконечно перезапускался с
`Connection to node 1 (integration-bus-kafka/10.96.x.x:9093) could not be established`.

**Причина:** `KAFKA_CONTROLLER_QUORUM_VOTERS=1@integration-bus-kafka:9093` заставляет единственный брокер
подключаться **к самому себе** через Service DNS-имя `integration-bus-kafka`, чтобы установить KRaft-кворум. Но
обычный `ClusterIP`-сервис маршрутизирует трафик только на поды в статусе **Ready** — а под не может стать Ready,
пока не подключится сам к себе. Замкнутый круг. В docker-compose эта проблема никогда не проявлялась, потому что
встроенный DNS Docker резолвит имя контейнера напрямую в его IP без привязки к healthcheck-статусу.

**Фикс:** Service Kafka сделан headless (`clusterIP: None`) с `publishNotReadyAddresses: true` — тогда DNS отдаёт
IP пода независимо от его готовности.

### 4.2. Таймаут liveness/readiness-проб короче времени старта JVM-утилиты

**Симптом:** Kafka попадала в `CrashLoopBackOff` с `Liveness probe failed: command timed out ... after 1s`.

**Причина:** `exec`-проба вызывает `kafka-broker-api-versions.sh` — обёртку над JVM, которой на старте JVM нужно
больше 1 секунды. Дефолтный `timeoutSeconds` для `exec`-проб в Kubernetes — **1 секунда** и без явного
переопределения его недостаточно для этой пробы.

**Фикс:** `timeoutSeconds: 10` на readiness- и liveness-пробах.

### 4.3. Отсутствие PersistentVolumeClaim у Kafka — потеря топиков при рестарте

**Симптом:** после ручного `kubectl delete pod integration-bus-kafka-0` (применение фикса §4.2) все топики,
созданные Job'ом `kafka-provisioner`, пропали, и все 6 .NET-сервисов упали с
`Confluent.Kafka.ConsumeException: Subscribed topic not available`.

**Причина:** `/tmp/kafka-logs` (реальный `log.dirs` у этого образа) не был смонтирован ни на один `PersistentVolumeClaim`
— это обычная файловая система контейнера, которая обнуляется при каждом пересоздании пода.

**Фикс:** добавлен `volumeClaimTemplates` для Kafka (как уже было у Postgres/Redis/ClickHouse/Loki).

> `volumeClaimTemplates` в Kubernetes неизменяем после создания `StatefulSet` — добавить его к уже существующему
> стейтфулсету нельзя, только пересоздать ресурс. Для демонстрационного/дев-кластера проще всего удалить и заново
> создать namespace (`kubectl delete namespace integration-bus`), что и было сделано для финальной чистой проверки.

### 4.4. `kind load docker-image` не работает для публичных образов

**Симптом:** `ERROR: failed to load image: ... ctr: content digest sha256:... not found` для всех образов, кроме
наших 7 самосборных.

**Причина:** известная несовместимость `kind load docker-image` с containerd-backed image store, который Docker
Desktop использует по умолчанию в новых версиях — `docker save` не сериализует все content-блобы, на которые
ссылается многоплатформенный манифест.

**Обход:** для публичных образов `kind load` не нужен вовсе — подам достаточно `imagePullPolicy: IfNotPresent`
(или дефолтного поведения), и containerd на ноде Kind просто скачивает образ из интернета напрямую при первом
поступлении пода. Актуально только для наших 7 кастомных образов, у которых нет внешнего реестра.

### 4.5. Нестабильность control-plane при одновременной работе с docker-compose

**Симптом:** `kube-scheduler` и `kube-controller-manager` входили в `CrashLoopBackOff` с
`context deadline exceeded` при обращении к `etcd`/`kube-apiserver`; из-за этого контроллер `Endpoints` не успевал
вовремя помечать `clickhouse` живым, несмотря на прошедший readiness.

**Причина:** одновременный запуск полного docker-compose стека (18 контейнеров) **и** полного Kind-кластера
(ещё 18 workload'ов + control-plane) на одной машине перегружал диск/CPU настолько, что компоненты control-plane
не успевали обновлять lease для leader election.

**Обход:** `docker compose down` (без `-v`, volume'ы сохранены) перед финальной проверкой K8s-стека. Оба стека
**не предназначены для одновременной работы** на одной машине с ограниченными ресурсами — это ожидаемое
архитектурное ограничение локальной разработки, а не баг чарта.

### 4.6. Самовосстанавливающийся race condition на первом старте

**Наблюдение (не баг, задокументировано как есть):** на первом развёртывании 1–2 из 6 .NET-сервисов падают с
`Subscribed topic not available` и автоматически перезапускаются Kubernetes'ом (`restartPolicy: Always`), потому
что их собственный под стартует чуть раньше, чем `kafka-provisioner` Job успевает создать все топики. Это
безопасно: после 1–2 рестартов (обычно меньше минуты) все поды стабильно переходят в `1/1 Running` без ручного
вмешательства, так как топики уже существуют к моменту следующей попытки подключения.

---

## 5. Инфраструктурные файлы

ConfigMap'ы чарта встраивают содержимое `infrastructure/postgres/init.sql`, `infrastructure/clickhouse/init.sql`,
`infrastructure/prometheus/prometheus.yml`, `infrastructure/debezium/*.json` + `register-connectors.sh`,
`infrastructure/clickhouse-sink/*.json` через `.Files.Get` над их копиями в `deploy/k8s/charts/integration-bus/files/`.
Helm не умеет читать файлы вне директории чарта, поэтому копии неизбежны — **при изменении оригиналов в
`infrastructure/` не забывайте синхронизировать копии в `deploy/k8s/charts/integration-bus/files/`.**

---

## 6. Проверено вживую

Полный «чёрный ящик» (как в [`docs/business-logic/validation-guide.ru.md`](../business-logic/validation-guide.ru.md)), выполненный через проброшенные Kind-порты на чистом,
только что созданном namespace:
1. `POST /api/v1/accounts/seed` → `202`, счета появились в Postgres.
2. `POST /api/v1/accounts/{id}/topup` → `202`.
3. `POST /api/v1/ledger/transaction` → `202`, сага дошла до `CurrentState = Completed`, `ErrorMessage = NULL`.
4. `SELECT * FROM analytics.transaction_cube WHERE TransactionId = '...'` в ClickHouse — строка на месте, со
   всеми верными полями (включая прошедший полный цикл Debezium → Kafka Connect → ClickHouse Sink).
5. `GET /api/services` в Jaeger — все 6 .NET-сервисов видны как источники трейсов.

---

## 7. Остановка и очистка

```bash
helm uninstall integration-bus -n integration-bus
kubectl delete namespace integration-bus   # также удаляет все PVC
kind delete cluster --name integration-bus # полностью снести кластер
```
