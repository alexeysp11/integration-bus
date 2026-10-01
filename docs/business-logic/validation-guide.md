# 🎯 Validation Guide: сквозной сценарий проверки «Black Box»

Пошаговый сценарий для инженера, который впервые запускает систему и должен убедиться, что весь стек — от HTTP-запроса
до аналитической витрины в ClickHouse — работает целиком. Все команды — `bash`/`cURL`, без предварительных знаний о
внутреннем устройстве кода.

---

## Шаг 1. Запуск и проверка здоровья системы

```bash
cd integration-bus
docker compose up -d
```

Дождитесь, пока одноразовые init-контейнеры завершатся (`Exited (0)`), остальные — в состоянии `Up`:

```bash
docker compose ps
```

Проверьте health-эндпоинты всех шести .NET-сервисов (каждый должен вернуть `200 OK` / `Healthy`):

```bash
curl -s -o /dev/null -w "Gateway: %{http_code}\n"            http://localhost:5038/health
curl -s -o /dev/null -w "Processing.Api: %{http_code}\n"     http://localhost:5201/health
curl -s -o /dev/null -w "SagaOrchestrator: %{http_code}\n"   http://localhost:6001/health
curl -s -o /dev/null -w "AccountBalance: %{http_code}\n"     http://localhost:6002/health
curl -s -o /dev/null -w "Compliance: %{http_code}\n"         http://localhost:6003/health
curl -s -o /dev/null -w "CoreLedger: %{http_code}\n"         http://localhost:6004/health
```

Если какой-то сервис ещё `Unhealthy` — подождите 10–20 секунд (миграции БД и провижининг Kafka-топиков на первом
старте) и повторите. Статус всех 6 Kafka Connect коннекторов (3 Debezium source + 3 ClickHouse sink) должен быть
`RUNNING`:

```bash
curl -s "http://localhost:8083/connectors?expand=status"
```

---

## Шаг 2. Подготовка тестовых счетов

В системе нет отдельного публичного эндпоинта «создать один счёт» — счета создаются только через bulk-seed. Создадим
5 тестовых счетов в USD и возьмём два реальных `Id` из базы:

```bash
curl -s -X POST http://localhost:5038/api/v1/accounts/seed \
  -H "Content-Type: application/json" \
  -d '{"count": 5, "currency": 1}'
# -> 202 Accepted

sleep 2

docker exec integration-bus-db psql -U postgres -d accounting_db -t -c \
  "SELECT \"Id\" FROM \"Accounts\" ORDER BY \"CreatedAt\" DESC LIMIT 2;"
```

Сохраните два GUID из вывода как `SOURCE_ID` и `TARGET_ID`. Пополните баланс счёта-источника, чтобы хватило на
перевод (у только что засеянных счетов баланс — 0):

```bash
curl -s -X POST "http://localhost:5038/api/v1/accounts/$SOURCE_ID/topup" \
  -H "Content-Type: application/json" \
  -d '{"amount": 1000.00, "currency": 1}'
# -> 202 Accepted {"message":"Top-up request accepted...","trackingTransactionId":"..."}

sleep 2
```

---

## Шаг 3. Отправка тестовой транзакции

**Эндпоинт:** `POST http://localhost:5038/api/v1/ledger/transaction`
**Content-Type:** `application/json`

```bash
curl -s -X POST http://localhost:5038/api/v1/ledger/transaction \
  -H "Content-Type: application/json" \
  -d "{
    \"sourceAccountId\": \"$SOURCE_ID\",
    \"targetAccountId\": \"$TARGET_ID\",
    \"amount\": 100.00,
    \"currency\": 1
  }"
```

### Ожидаемый результат

**Статус-код:** `202 Accepted`

**Тело ответа:**
```json
{
  "transactionId": "b1111111-2222-3333-4444-999999999977",
  "status": "Processing",
  "message": "Your transaction payload has been accepted and queued for processing."
}
```

Сохраните `transactionId` из ответа — он понадобится на следующих шагах.

> **Нет отдельного GET-эндпоинта опроса статуса саги** (несмотря на то, что это упоминалось в ранних версиях
> `docs/roadmap.md`, в коде такого маршрута нет). Чтобы увидеть финальное состояние распределённой транзакции,
> черным ящиком запросите таблицу состояния саги напрямую:

```bash
sleep 3
docker exec integration-bus-db psql -U postgres -d saga_db -c \
  "SELECT \"CorrelationId\", \"CurrentState\", \"ErrorMessage\" FROM \"TransactionState\" WHERE \"CorrelationId\" = '<transactionId>';"
```

Ожидается `CurrentState = Completed` и `ErrorMessage = NULL` — сага прошла все 4 шага (Hold → Compliance → Ledger →
Confirm) успешно.

---

## Шаг 4. Проверка контура Observability

1. **Loki (логи).** Откройте Grafana → `http://localhost:3000` (логин/пароль `admin`/`admin`) → **Explore** →
   выберите datasource **Loki** (настройка — `docs/observability/README.md` §3.2) → запрос:
   ```logql
   {service_name="integration-bus-saga-orchestrator-service"} |= "<transactionId>"
   ```
   В найденных строках лога будет поле `TraceId` (добавлено `Serilog.Enrichers.Span`).

2. **Jaeger (трейсинг).** Скопируйте `TraceId` из лога и откройте:
   ```text
   http://localhost:16686/trace/<TraceId>
   ```
   Должна отобразиться waterfall-диаграмма со спанами минимум от `integration-bus-processing-api` и
   `integration-bus-saga-orchestrator-service` — сквозной путь сообщения через Kafka-топики саги.

3. **Prometheus (метрики).** Откройте `http://localhost:9090/graph` и выполните запрос:
   ```promql
   rate(http_server_request_duration_seconds_count{job="processing-api"}[5m])
   ```
   График должен показать ненулевой всплеск в момент отправки запроса из Шага 3 (сам набор экспортируемых метрик
   — см. `docs/observability/README.md` §4.2, автодополнение Prometheus подскажет точные имена).

---

## Шаг 5. Проверка аналитического контура (ClickHouse)

Через несколько секунд после завершения саги (Debezium → Kafka → ClickHouse Sink Connector, см.
`docs/data-analytics/README.ru.md`) измененные данные должны появиться в ClickHouse:

```bash
docker exec integration-bus-clickhouse clickhouse-client \
  --user default --password clickhouse_dev_password --query \
  "SELECT * FROM analytics.transaction_cube WHERE TransactionId = '<transactionId>' FORMAT Vertical"
```

Ожидаемый результат — одна строка с:
- `JournalEntryType = 1` (Hold) и `JournalAmountDelta = -100` (списание 100.00 со счёта-источника);
- `ComplianceStatus = 2` (Passed);
- `LedgerAmount = 100` и непустым `LedgerCommittedAt`.

Если строка не появилась — проверьте статусы sink-коннекторов (`curl -s http://localhost:8083/connectors/clickhouse-sink-ledger/status`)
и раздел «Как убедиться, что пайплайн работает» в `docs/data-analytics/README.ru.md` §4.

---

## Итог

Если все 5 шагов прошли успешно — вы проверили вживую весь контур: HTTP-прослойку (Gateway → Processing.Api),
распределённую сагу (SagaOrchestrator → AccountBalance → Compliance → CoreLedger) с распределённой блокировкой и
декларативными правилами, инфраструктурное HMAC-маскирование (параллельно пишется в `*.security`-топики, см.
`docs/reliability/README.ru.md`), стек наблюдаемости (Prometheus/Loki/Jaeger) и CDC-аналитику (Debezium → Kafka
Connect ClickHouse Sink → ClickHouse → Metabase).
