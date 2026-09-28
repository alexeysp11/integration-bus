# 📉 Real-Time Analytics Pipeline: Debezium + ClickHouse + Metabase

[English](README.md) | [Русский](README.ru.md)

Реализация по приоритетному описанию из [`docs/roadmap.md`](../roadmap.md) (Stage 4): **Kafka Connect + Debezium** для CDC-захвата
изменений из Postgres, и нативный **ClickHouse Kafka Engine + Materialized Views** для заливки в OLAP (а не
отдельный "ClickHouse Kafka Connect Sink"-коннектор — так поступил и `roadmap.md`, и это надёжнее: не требует
стороннего Kafka Connect sink-плагина и официально поддерживается ClickHouse "из коробки").

Весь пайплайн проверен вживую (`docker compose up -d`) и покрыт интеграционным тестом на Testcontainers.

---

## 1. Архитектура

```text
 Postgres (accounting_db / compliance_db / ledger_db, wal_level=logical)
         │  логическая репликация (pgoutput)
         ▼
 Kafka Connect (debezium/connect:3.0.0.Final)
   ├─ balance-db-connector      → топик cdc.balance.public.JournalEntries
   ├─ compliance-db-connector   → топик cdc.compliance.public.ComplianceAudits
   └─ ledger-db-connector       → топик cdc.ledger.public.LedgerEntries
         │  (Single Message Transform: ExtractNewRecordState — "разворачивает" Debezium-конверт в плоский JSON)
         ▼
 ClickHouse (Kafka Engine "очереди" → Materialized View → MergeTree)
   ├─ analytics.journal_entries_queue      → analytics.journal_entries_mv      → analytics.journal_entries
   ├─ analytics.compliance_audits_queue    → analytics.compliance_audits_mv    → analytics.compliance_audits
   ├─ analytics.ledger_entries_queue       → analytics.ledger_entries_mv       → analytics.ledger_entries
   └─ analytics.transaction_cube (VIEW, JOIN трёх таблиц по TransactionId)
         │
         ▼
 Metabase (официальный ClickHouse-драйвер, автозагружается при старте)
```

Новые контейнеры (`docker-compose.yml`): `integration-bus-kafka-connect` (порт `8083`),
`integration-bus-debezium-registrar` (одноразовый — регистрирует 3 коннектора через REST API и завершается),
`integration-bus-clickhouse` (`8123` HTTP, `9000` native), `integration-bus-metabase-driver-setup` (одноразовый —
скачивает `clickhouse.metabase-driver.jar`), `integration-bus-metabase` (`3001`, проброшен на внутренний `3000`).

`integration-bus-db` теперь стартует с `command: ["postgres", "-c", "wal_level=logical", "-c", "max_wal_senders=10", "-c", "max_replication_slots=10"]` — обязательное условие для логической репликации Debezium.

---

## 2. ⚠️ Важное расхождение с "очевидным" выбором версий (найдено и исправлено эмпирически)

При первом запуске `apache/kafka:latest` резолвился в **Kafka 4.3.1**. ClickHouse 24.8 (бандл `librdkafka`) не смог
подключиться к такому брокеру: `system.kafka_consumers` показывал ошибку
`Local: Required feature not supported by broker`. Kafka 4.x убрал поддержку части старых протокольных версий,
на которые рассчитан `librdkafka`, зашитый в ClickHouse 24.8.

**Фикс:** брокер зафиксирован на `apache/kafka:3.9.0` (последняя версия ветки 3.x, полностью совместимая и с
ClickHouse, и с Debezium, и с `kafka-ui`). Если увидите в `system.kafka_consumers` ту же ошибку — значит, кто-то
снова переключил образ брокера на `:latest`.

Второй нюанс: Debezium с `time.precision.mode=connect` и `JsonConverter(schemas.enable=false)` сериализует
Postgres `timestamp` не как epoch-число, а как **ISO-8601 строку** (`"2026-09-28T09:41:12.842158Z"`). Поэтому все
`_queue`-таблицы в ClickHouse объявляют временные поля как `String`, а Materialized View конвертирует их через
`parseDateTime64BestEffort(..., 6)`, а не `fromUnixTimestamp64Milli(...)`.

---

## 3. Схема данных в ClickHouse

Полное DDL — `infrastructure/clickhouse/init.sql`. Ключевые таблицы:

| Таблица | Тип | Назначение |
|---|---|---|
| `analytics.journal_entries_queue` / `..._mv` / `journal_entries` | Kafka / MV / MergeTree | Балансовые проводки (holds/releases/confirms/top-ups) из `accounting_db.JournalEntries` |
| `analytics.compliance_audits_queue` / `..._mv` / `compliance_audits` | Kafka / MV / MergeTree | Аудит комплаенс-проверок из `compliance_db.ComplianceAudits` |
| `analytics.ledger_entries_queue` / `..._mv` / `ledger_entries` | Kafka / MV / MergeTree | Финальные проводки леджера из `ledger_db.LedgerEntries` |
| `analytics.transaction_cube` | VIEW | JOIN трёх таблиц по `TransactionId` — плоский срез для отчётности |

`MergeTree`-таблицы отсортированы по `(AccountId/TransactionId, ...)` — типичный ключ для аналитических выборок
по счёту/транзакции. `_queue`-таблицы (`ENGINE = Kafka`) не предназначены для прямых `SELECT` — они существуют
только чтобы материализация могла читать поток; вся отчётность строится над MergeTree-таблицами и `transaction_cube`.

---

## 4. Как убедиться, что пайплайн работает

```bash
docker compose up -d
# дождаться, пока integration-bus-debezium-registrar и integration-bus-metabase-driver-setup завершатся (Exited (0))
docker compose ps

# статус всех трёх Debezium-коннекторов должен быть RUNNING
curl -s http://localhost:8083/connectors?expand=status
```

Сквозная проверка (вставка напрямую в Postgres — так же работает и через реальный бизнес-флоу API):

```bash
docker exec integration-bus-db psql -U postgres -d ledger_db -c \
  "INSERT INTO \"LedgerEntries\" (\"TransactionId\", \"Amount\", \"CreatedAt\") VALUES (gen_random_uuid(), 777.25, now());"

# через несколько секунд:
docker exec integration-bus-clickhouse clickhouse-client --query \
  "SELECT * FROM analytics.ledger_entries ORDER BY Id DESC LIMIT 1 FORMAT Vertical"
```

Если строка не появляется — проверьте `system.kafka_consumers`:

```bash
docker exec integration-bus-clickhouse clickhouse-client --query \
  "SELECT database, table, exceptions.text[1] FROM system.kafka_consumers FORMAT Vertical"
```

---

## 5. Подключение Metabase к ClickHouse

Официальный open-source Metabase не включает ClickHouse-драйвер по умолчанию — `docker-compose.yml` сам скачивает
его (`integration-bus-metabase-driver-setup`, файл `clickhouse.metabase-driver.jar` от `ClickHouse/metabase-clickhouse-driver`)
в общий volume `/plugins`, так что никаких ручных шагов для установки драйвера не требуется.

1. Откройте `http://localhost:3001` и пройдите начальную настройку Metabase (создание аккаунта администратора —
   любые тестовые данные, e-mail подтверждать не нужно).
2. На экране "Add your data" (или позже: **Settings (шестерёнка) → Admin settings → Databases → Add database**)
   в выпадающем списке **Database type** выберите **ClickHouse**.
3. Заполните поля подключения:
   * **Host**: `integration-bus-clickhouse` (имя контейнера — Metabase обращается к нему внутри общей Docker-сети).
   * **Port**: `8123`.
   * **Database name**: `analytics`.
   * **Username**: `default`.
   * **Password**: оставьте пустым (аутентификация не настроена).
4. Нажмите **Save**, затем **Sync database schema now** — Metabase подтянет все таблицы `analytics.*`.
5. Для построения дашборда: **New → Question → выбрать базу `analytics`** → таблица `transaction_cube` (готовый
   плоский срез) → добавьте нужные агрегации (например, сумму `LedgerAmount` по дням) → **Visualize** →
   **Save** → добавьте вопрос в новый или существующий **Dashboard**.

---

## 6. Тестирование

`tests/IntegrationBus.Analytics.Tests/LedgerCdcPipelineTests.cs` — интеграционный тест на `Testcontainers`
(Postgres + Kafka + Kafka Connect/Debezium + ClickHouse, все в одной Docker-сети через `Testcontainers.Networks`):
1. Поднимает все 4 контейнера, создаёт таблицу `LedgerEntries`, регистрирует тот же Debezium-коннектор и ту же
   ClickHouse-схему (Kafka Engine + MV), что и в `docker-compose.yml`/`infrastructure/clickhouse/init.sql`.
2. Вставляет строку в Postgres.
3. Через `Policy.Handle<Exception>().WaitAndRetryAsync(...)` (Polly) поллит ClickHouse до появления строки.
4. Проверяет **полную идентичность** реплицированных полей (`Amount`, `CreatedAt`) исходной вставке.

**Важно:** тест явно фиксирует образ `clickhouse/clickhouse-server:24.8` — версия по умолчанию у
`Testcontainers.ClickHouse` (`23.6.3`) не содержит `system.kafka_consumers` и не совместима с современным брокером
Kafka по той же причине, что описана в п. 2.
