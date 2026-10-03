# 📊 Observability Stack: Prometheus, Grafana, Loki, Jaeger

[English](README.md) | [Русский](README.ru.md)

Пошаговая инструкция для инженера уровня Production по проверке и настройке стека наблюдаемости `integration-bus`.

---

## 1. Что добавлено

| Компонент | Контейнер | Порт (host) | Назначение |
|---|---|---|---|
| Prometheus | `integration-bus-prometheus` | `9090` | Сбор метрик (`/metrics`) со всех 6 .NET-сервисов |
| Grafana | `integration-bus-grafana` | `3000` | Дашборды, Data Sources для Prometheus/Loki/Jaeger |
| Jaeger (all-in-one) | `integration-bus-jaeger` | `16686` (UI), `4317` (OTLP gRPC), `4318` (OTLP HTTP) | Распределённый трейсинг |
| Loki | `integration-bus-loki` | `3100` | Централизованное хранилище логов |

В коде (`IntegrationBus.Shared.Extensions.TelemetryExtensions`) добавлен метод `AddDistributedTracing()`, который регистрирует:
- `AddSource("MassTransit")` — трейсинг Kafka/MassTransit consume/produce операций;
- `AddAspNetCoreInstrumentation()` — входящие HTTP-запросы;
- `AddHttpClientInstrumentation()` — исходящие HTTP-вызовы (например, Gateway → Processing.Api);
- `AddEntityFrameworkCoreInstrumentation()` — SQL-команды Postgres;
- `AddOtlpExporter()` — экспорт в Jaeger по адресу из переменной окружения `OTEL_EXPORTER_OTLP_ENDPOINT` (уже проставлена в `docker-compose.yml` как `http://integration-bus-jaeger:4317` для каждого сервиса).

Логи каждого сервиса пишутся в три приёмника Serilog (`Console`, `File`, `GrafanaLoki`), обогащённые `TraceId`/`SpanId` через `Serilog.Enrichers.Span` (`Enrich: ["FromLogContext", "WithSpan"]` в `appsettings.json`), что позволяет напрямую переходить от строки лога к трейсу в Jaeger.

Каждый сервис также экспонирует `GET /health` (через `Microsoft.Extensions.Diagnostics.HealthChecks` + `AspNetCore.HealthChecks.NpgSql/Kafka/Redis`), проверяющий реальное соединение с его зависимостями (Postgres, Kafka, а для `account-balance-service`/`core-ledger-service` — ещё и Redis).

---

## 2. Как убедиться, что контейнеры запустились успешно

```bash
docker compose up -d
docker compose ps
```

Все сервисы должны быть в состоянии `Up` (для `integration-bus-db`, `integration-bus-redis`, `integration-bus-kafka` — `Up (healthy)`; `integration-bus-kafka-provisioner` — `Exited (0)`, это ожидаемо, он одноразовый).

Проверка health-эндпоинтов приложений с хоста:

```bash
curl http://localhost:5038/health   # Gateway.Api
curl http://localhost:5201/health   # Processing.Api
curl http://localhost:6001/health   # Saga Orchestrator
curl http://localhost:6002/health   # Account Balance
curl http://localhost:6003/health   # Compliance
curl http://localhost:6004/health   # Core Ledger
```

Каждый должен вернуть `Healthy` с кодом `200 OK`. Если какой-то возвращает `Unhealthy`/`503`, это значит, что соответствующая зависимость (Postgres/Kafka/Redis) ещё не готова — подождите несколько секунд и повторите (Kafka-провижининг и миграции БД выполняются при первом старте).

Проверка Prometheus targets — откройте `http://localhost:9090/targets`: все 5 job'ов (`processing-api`, `saga-orchestrator`, `account-balance`, `compliance`, `core-ledger`) должны иметь статус `UP`.

Проверка Jaeger — `http://localhost:16686` должен открыть UI без ошибок.

Проверка Loki — `curl http://localhost:3100/ready` должен вернуть `ready`.

---

## 3. Настройка Grafana: подключение Data Sources

Откройте `http://localhost:3000` (логин/пароль: `admin` / `admin`, Grafana попросит сменить пароль при первом входе — можно пропустить кнопкой **Skip**).

### 3.1 Prometheus
1. В левом меню нажмите на иконку шестерёнки **Connections → Data sources**.
2. Нажмите **Add data source**.
3. Выберите **Prometheus** из списка.
4. В поле **URL** введите `http://integration-bus-prometheus:9090` (используется имя контейнера, так как Grafana обращается к Prometheus внутри общей Docker-сети `integration-bus-network`, а не через `localhost`).
5. Нажмите **Save & Test** внизу страницы. Должно появиться зелёное сообщение `Successfully queried the Prometheus API`.

### 3.2 Loki
1. **Connections → Data sources → Add data source → Loki**.
2. В поле **URL** введите `http://integration-bus-loki:3100`.
3. Нажмите **Save & Test** — ожидается `Data source successfully connected`.

### 3.3 Jaeger
1. **Connections → Data sources → Add data source → Jaeger**.
2. В поле **URL** введите `http://integration-bus-jaeger:16686`.
3. Нажмите **Save & Test**.

> После настройки Jaeger как data source, в Grafana Explore на панели Loki у логов, обогащённых `TraceId`, автоматически появляется кнопка перехода к соответствующему трейсу (derived field), если включить его в настройках Loki data source (**Loki → Settings → Derived fields → Add** → `Name: TraceId`, `Regex: "TraceId":"(\w+)"`, `Query: ${__value.raw}`, **Internal link → Jaeger**).

---

## 4. Импорт базовых дашбордов

### 4.1 Через готовые community-дашборды
1. В левом меню откройте **Dashboards → New → Import**.
2. В поле **Import via grafana.com** введите ID дашборда, который хотите импортировать (найдите актуальный на [grafana.com/grafana/dashboards](https://grafana.com/grafana/dashboards/), отфильтровав по datasource `Prometheus` и ключевым словам `ASP.NET Core` или `.NET`/`OpenTelemetry`).
3. На следующем шаге в выпадающем списке **Prometheus** выберите datasource, созданный в п. 3.1, и нажмите **Import**.

Готовые ID дашбордов на grafana.com периодически меняются/устаревают, поэтому не полагайтесь на жёстко захардкоженный номер — safer вариант ниже.

### 4.2 Безопасный вариант: собрать минимальный дашборд вручную по реальным метрикам
1. **Dashboards → New → New Dashboard → Add visualization** → выберите datasource **Prometheus**.
2. В строке запроса начните вводить `http_server` — Prometheus покажет автодополнение по фактическим метрикам, экспортируемым `OpenTelemetry.Instrumentation.AspNetCore` (например, длительность и количество HTTP-запросов по каждому сервису).
3. Аналогично введите `process_runtime_dotnet` — метрики GC/heap/threads от `OpenTelemetry.Instrumentation.Runtime` (память, потоки, сборки мусора).
4. Введите `masstransit` — метрики консьюмеров/продюсеров MassTransit (число обработанных сообщений, длительность обработки, количество ошибок) от `AddMeter("MassTransit")`.
5. Сохраните дашборд (**Save dashboard**), задав имя, например `integration-bus: Runtime & MassTransit Overview`.

Так как имена метрик, отдаваемые версией пакетов `OpenTelemetry.Instrumentation.*`, зафиксированной в проекте (см. `src/Libraries/IntegrationBus.Shared/IntegrationBus.Shared.csproj`), могут отличаться от того, что показывает случайно найденный community-дашборд, автодополнение Prometheus в п. 4.2 — самый надёжный способ увидеть именно те метрики, которые реально экспортируются этим решением.

---

## 5. Сквозная проверка: лог → трейс → метрика

1. Отправьте тестовую транзакцию (см. [`docs/business-logic/validation-guide.ru.md`](../business-logic/validation-guide.ru.md) либо [`docs/business-logic/api-specifications.md`](../business-logic/api-specifications.md) для готового `cURL`).
2. Скопируйте `transactionId` из ответа.
3. В Grafana → **Explore** → выберите datasource **Loki** → запрос `{service_name="integration-bus-saga-orchestrator-service"} |= "<transactionId>"`.
4. У найденной строки лога найдите `TraceId` (добавлен `Serilog.Enrichers.Span`) и откройте его в Jaeger (`http://localhost:16686/trace/<TraceId>`), либо через derived field, настроенный в п. 3.3.
5. В Jaeger должна отобразиться цепочка спанов через все сервисы, участвовавшие в саге (`Processing.Api → SagaOrchestrator → AccountBalance → Compliance → CoreLedger`).

Результат: единый `TraceId` совпадает в логах всех задействованных сервисов в Loki, а соответствующий трейс в
Jaeger содержит полную цепочку из 6 сервисов (`gateway-api → processing-api → saga-orchestrator-service →
account-balance-service → compliance-service → core-ledger-service`), включая Kafka `send`/`receive`/`process`-спаны,
3 execute-активности Courier Routing Slip (`WriteAuditTrail`, `UpdateCache`, `PublishLedgerCommitted`) и `saga_db`
(Postgres) спаны.

`SagaOrchestrator.Service` логирует каждый из 4 шагов саги на уровне `Information` (диспатч `HoldAccountBalance`,
`CheckComplianceLimits`, `WriteLedgerRecord`, `ConfirmAccountBalance`) и на уровне `Warning` — каждую компенсацию и
технический фолт `ProcessLedgerWriteActivity`, так что центральный оркестратор полностью виден в Loki/Grafana
наравне с остальными сервисами.

### Troubleshooting: сага выполняется, но не видна в Loki

Если новая Activity саги молчит в логах, несмотря на корректное выполнение — проверьте, что в её конструктор
инжектирован `ILogger<T>` и внутри `Execute`/`Faulted` есть явный вызов `logger.LogInformation`/`LogWarning`:
MassTransit не логирует сами по себе шаги State Machine, это ответственность конкретной `IStateMachineActivity`.
