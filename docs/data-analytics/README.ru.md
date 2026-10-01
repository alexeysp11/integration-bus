# 📉 Real-Time Analytics Pipeline: Debezium + ClickHouse + Metabase

[English](README.md) | [Русский](README.ru.md)

Реализация по приоритетному описанию из [`docs/roadmap.md`](../roadmap.md) (Stage 4): **Kafka Connect + Debezium**
для CDC-захвата изменений из Postgres, и официальный **ClickHouse Kafka Connect Sink Connector** для заливки в
OLAP.

> **Почему не нативный ClickHouse Kafka Engine.** Первая версия этого пайплайна использовала `ENGINE = Kafka` +
> Materialized View — рабочий, но архитектурно слабый вариант: он читает и вставляет построчно, что на
> MergeTree-семье движков приводит к взрывному росту мелких кусков (parts) под нагрузкой, и не даёт контроля над
> тем, когда Kafka-offset считается "подтверждённым". Разбор проблемы и альтернатив — в [`docs/data-loading.ru.md`](../data-loading.ru.md).
> Официальный sink-коннектор решает оба вопроса: он батчит вставки и коммитит offset в Kafka **только после**
> подтверждения записи в ClickHouse (at-least-once), а повторно присланные (при ретрае) батчи схлопываются
> механизмом блочной дедупликации ClickHouse (`non_replicated_deduplication_window`) в связке с
> `ReplacingMergeTree` — то есть финальная консистентность гарантирована без costly `FINAL`-модификатора на каждый
> `SELECT`.

Весь пайплайн проверен вживую (`docker compose up -d`) и покрыт интеграционным тестом на Testcontainers.

---

## 1. Архитектура

```text
 Postgres (accounting_db / compliance_db / ledger_db, wal_level=logical)
         │  логическая репликация (pgoutput)
         ▼
 Kafka Connect (кастомный образ: debezium/connect + официальный ClickHouse Kafka Connect Sink plugin)
   ├─ balance-db-connector      → топик cdc.balance.public.JournalEntries      ─┐
   ├─ compliance-db-connector   → топик cdc.compliance.public.ComplianceAudits ─┤ (Debezium source,
   └─ ledger-db-connector       → топик cdc.ledger.public.LedgerEntries       ─┘  SMT: ExtractNewRecordState)
         │
         ▼ (тот же Kafka Connect worker, те же топики)
   ├─ clickhouse-sink-balance      (topic2TableMap → analytics.journal_entries)
   ├─ clickhouse-sink-compliance   (topic2TableMap → analytics.compliance_audits)
   └─ clickhouse-sink-ledger       (topic2TableMap → analytics.ledger_entries)
         │  батчинг + at-least-once, offset коммитится только после ACK от ClickHouse
         ▼
 ClickHouse (analytics.*: ReplacingMergeTree + non_replicated_deduplication_window)
   └─ analytics.transaction_cube (VIEW, JOIN трёх таблиц по TransactionId)
         │
         ▼
 Metabase (официальный ClickHouse-драйвер, автозагружается при старте)
```

Новые контейнеры (`docker-compose.yml`): `integration-bus-kafka-connect` (порт `8083`, собирается из
`infrastructure/kafka-connect/Dockerfile` — `debezium/connect:3.0.0.Final` + официальный плагин
`ClickHouse/clickhouse-kafka-connect`), `integration-bus-debezium-registrar` (одноразовый — регистрирует все 6
коннекторов через REST API: 3 Debezium source из `infrastructure/debezium/`, 3 ClickHouse sink из
`infrastructure/clickhouse-sink/`), `integration-bus-clickhouse` (`8123` HTTP, `9000` native),
`integration-bus-metabase-driver-setup` (одноразовый — скачивает `clickhouse.metabase-driver.jar`),
`integration-bus-metabase` (`3001`, проброшен на внутренний `3000`).

`integration-bus-db` стартует с `command: ["postgres", "-c", "wal_level=logical", "-c", "max_wal_senders=10", "-c", "max_replication_slots=10"]` — обязательное условие для логической репликации Debezium.

---

## 2. ⚠️ Пять неочевидных проблем, найденных и исправленных эмпирически

При разворачивании этого пайплайна вживую вскрылись пять независимых проблем — ни одна не была видна из
документации заранее, все найдены через реальные логи контейнеров и реальный бизнес-флоу (`docs/validation-guide.md`),
а не только точечные ручные INSERT'ы:

**1. `apache/kafka:latest` → Kafka 4.3.1 несовместим с ClickHouse 24.8.** ClickHouse (бандл `librdkafka`) не мог
подключиться: ошибка `Local: Required feature not supported by broker`. Kafka 4.x убрал поддержку части старых
протокольных версий. **Фикс:** брокер зафиксирован на `apache/kafka:3.9.0`.

**2. Официальный образ ClickHouse без `CLICKHOUSE_PASSWORD` отключает сетевой доступ для `default`.** В логах
контейнера: `neither CLICKHOUSE_USER nor CLICKHOUSE_PASSWORD is set, disabling network access for user 'default'`.
Локальный `clickhouse-client` внутри контейнера продолжает работать (это сбивало с толку при ручной проверке), но
HTTP-запросы от Kafka Connect/Metabase отклоняются с `AUTHENTICATION_FAILED`. **Фикс:** задан
`CLICKHOUSE_PASSWORD=clickhouse_dev_password`, тот же пароль — в конфигах sink-коннекторов.

**3. Плагин `clickhouse-kafka-connect` v1.7.0 по умолчанию использует `client_version=V1` — он падает на
`ping()` без видимой причины** (`Unable to ping ClickHouse instance`, retries мгновенные, без стектрейса).
**Фикс:** явно `"client_version": "V2"` в каждом sink-коннекторе — современный клиент, который реально
поддерживается проектом.

**4. `TIMESTAMP` vs `TIMESTAMP WITH TIME ZONE` в Postgres меняет формат, в котором Debezium шлёт время.**
С `time.precision.mode=connect`: обычный `timestamp` (без зоны) сериализуется как **число** (epoch-миллисекунды,
логический тип Kafka Connect `Timestamp`), а `timestamptz` — как **ISO-8601 строка** (например,
`"2026-03-14T08:30:00.123456Z"`). Все три продакшен-таблицы (`JournalEntries.TimestampUtc`,
`ComplianceAudits.CreatedAtUtc`, `LedgerEntries.CreatedAt`) используют `timestamp with time zone` (проверено по
EF Core миграциям), поэтому везде приходит строка. Чтобы ClickHouse принял такую строку в колонку `DateTime64(6)`,
sink-коннектору нужна настройка `"clickhouseSettings": "date_time_input_format=best_effort"` — без неё строгий
парсер ClickHouse падает с `CANNOT_PARSE_INPUT_ASSERTION_FAILED`. Если в какой-то новой таблице случайно
используют `timestamp` без зоны — дата в ClickHouse окажется в **1970 году** (значение молча трактуется как число),
без единой ошибки в логах — явный повод для regression-теста при добавлении новых источников.

**5. Гонка при старте Kafka Connect REST API.** Регистратор (`register-connectors.sh`) проверял готовность через
`GET /connectors`, но воркер иногда отвечает на этот запрос ДО того, как полностью присоединится к Connect-кластеру,
и первый же `POST /connectors` падает с "пустым ответом" (curl exit 52), обрывая `set -e`-скрипт и оставляя
инфраструктуру без зарегистрированных коннекторов. **Фикс:** добавлена пауза в 5 секунд после первой успешной
проверки готовности и retry-цикл (до 10 попыток с паузой 3с) вокруг каждой отдельной регистрации — так `docker
compose up -d` надёжно поднимает инфраструктуру "с одного раза" на чистой машине, как того требует Definition of
Done в `docs/roadmap.md`.

---

## 3. Схема данных в ClickHouse

Полное DDL — `infrastructure/clickhouse/init.sql`. Ключевые таблицы:

| Таблица | Движок | Назначение |
|---|---|---|
| `analytics.journal_entries` | ReplacingMergeTree | Балансовые проводки (holds/releases/confirms/top-ups) из `accounting_db.JournalEntries` |
| `analytics.compliance_audits` | ReplacingMergeTree | Аудит комплаенс-проверок из `compliance_db.ComplianceAudits` |
| `analytics.ledger_entries` | ReplacingMergeTree | Финальные проводки леджера из `ledger_db.LedgerEntries` |
| `analytics.transaction_cube` | VIEW | JOIN трёх таблиц по `TransactionId` — плоский срез для отчётности |

Каждая таблица объявлена с `SETTINGS non_replicated_deduplication_window = 100` (локальный, нереплицированный
аналог блочной дедупликации) и хранит сырое поле `__deleted` (строка `"true"`/`"false"` от Debezium SMT
`ExtractNewRecordState`), чтобы hard-delete из Postgres был виден, а не тихо отвергнут валидацией схемы
sink-коннектора. `ORDER BY` подобран под типичные аналитические выборки по счёту/транзакции.

Коннекторы (`infrastructure/clickhouse-sink/*.json`) используют `topic2TableMap`, сопоставляя CDC-топик с
конкретной таблицей, и общий набор настроек: `client_version=V2`, `exactlyOnce=false` (полагаемся на блочную
дедупликацию, не на ClickHouse Keeper), `clickhouseSettings=date_time_input_format=best_effort`,
`key.converter.schemas.enable=false`/`value.converter.schemas.enable=false` (должны быть выставлены явно —
значения по умолчанию воркера Kafka Connect приводят к ошибке `JsonConverter with schemas.enable requires
"schema" and "payload" fields`).

---

## 4. Как убедиться, что пайплайн работает

```bash
docker compose up -d
# дождаться, пока integration-bus-debezium-registrar и integration-bus-metabase-driver-setup завершатся (Exited (0))
docker compose ps

# статус всех 6 коннекторов (3 Debezium source + 3 ClickHouse sink) должен быть RUNNING
curl -s "http://localhost:8083/connectors?expand=status"
```

Сквозная проверка (вставка напрямую в Postgres — так же работает и через реальный бизнес-флоу API):

```bash
docker exec integration-bus-db psql -U postgres -d ledger_db -c \
  "INSERT INTO \"LedgerEntries\" (\"TransactionId\", \"Amount\", \"CreatedAt\") VALUES (gen_random_uuid(), 777.25, now());"

# через несколько секунд:
docker exec integration-bus-clickhouse clickhouse-client --user default --password clickhouse_dev_password --query \
  "SELECT * FROM analytics.ledger_entries ORDER BY Id DESC LIMIT 1 FORMAT Vertical"
```

Если строка не появляется — проверьте статус конкретной таски и, если `FAILED`, читайте `trace` в ответе:

```bash
curl -s "http://localhost:8083/connectors/clickhouse-sink-ledger/status"
# после правки конфигурации:
curl -s -X POST "http://localhost:8083/connectors/clickhouse-sink-ledger/restart?includeTasks=true"
```

Идемпотентность (повторная доставка не создаёт дубликат) проверяется так:

```bash
curl -s -X POST "http://localhost:8083/connectors/clickhouse-sink-ledger/restart?includeTasks=true"
docker exec integration-bus-clickhouse clickhouse-client --user default --password clickhouse_dev_password --query \
  "OPTIMIZE TABLE analytics.ledger_entries FINAL"
# count() по TransactionId должен остаться прежним
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
   * **Password**: `clickhouse_dev_password` (см. п. 2 выше — без пароля `default` недоступен по сети).
4. Нажмите **Save**, затем **Sync database schema now** — Metabase подтянет все таблицы `analytics.*`.
5. Для построения дашборда: **New → Question → выбрать базу `analytics`** → таблица `transaction_cube` (готовый
   плоский срез) → добавьте нужные агрегации (например, сумму `LedgerAmount` по дням) → **Visualize** →
   **Save** → добавьте вопрос в новый или существующий **Dashboard**.

---

## 6. Тестирование

`tests/IntegrationBus.Analytics.Tests/LedgerCdcPipelineTests.cs` — интеграционный тест на `Testcontainers`
(Postgres + Kafka + Kafka Connect/Debezium+Sink + ClickHouse, все в одной Docker-сети через
`Testcontainers.Networks`):
1. Собирает **тот же кастомный образ** Kafka Connect, что и `docker-compose.yml`
   (`ImageFromDockerfileBuilder` → `infrastructure/kafka-connect/Dockerfile`), поднимает все контейнеры, создаёт
   таблицу `LedgerEntries` (**обязательно** `TIMESTAMP WITH TIME ZONE`, как в реальной EF Core миграции — см.
   находку №4 в п. 2), регистрирует тот же Debezium source-коннектор и тот же ClickHouse sink-коннектор
   (`client_version=V2`, `date_time_input_format=best_effort`), создаёт whitelisted ClickHouse-таблицу
   (`ReplacingMergeTree`), что и в `infrastructure/clickhouse/init.sql`.
2. Вставляет строку в Postgres.
3. Через `Policy.Handle<Exception>().WaitAndRetryAsync(...)` (Polly) поллит ClickHouse до появления строки.
4. Проверяет **полную идентичность** реплицированных полей (`Amount`, `CreatedAt`) исходной вставке.

**Важно:** тест явно фиксирует образ `clickhouse/clickhouse-server:24.8` — версия по умолчанию у
`Testcontainers.ClickHouse` (`23.6.3`) несовместима с современным брокером Kafka по причине №1 из п. 2.
