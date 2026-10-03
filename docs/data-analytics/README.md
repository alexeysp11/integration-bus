# 📉 Real-Time Analytics Pipeline: Debezium + ClickHouse + Metabase

[English](README.md) | [Русский](README.ru.md)

Implemented per the priority description in [`docs/roadmap.md`](../roadmap.md) (Stage 4): **Kafka Connect +
Debezium** for CDC capture of changes from Postgres, and the official **ClickHouse Kafka Connect Sink Connector**
for loading into the OLAP store.

> **Why not the native ClickHouse Kafka Engine.** An alternative approach — `ENGINE = Kafka` + a Materialized
> View — is architecturally weaker: it reads and inserts row by row, which on the MergeTree family of engines
> causes an explosive growth in small parts under load, and gives no control over when a Kafka offset is
> considered "confirmed". A breakdown of this and other alternatives considered is in
> [`docs/data-analytics/data-loading.md`](data-loading.md). The official sink connector solves both issues: it
> batches inserts and commits the Kafka offset **only after** the write to ClickHouse is confirmed
> (at-least-once), and re-sent batches (on retry) are collapsed by ClickHouse's block-level deduplication
> mechanism (`non_replicated_deduplication_window`) in combination with `ReplacingMergeTree` — meaning eventual
> consistency is guaranteed without a costly `FINAL` modifier on every `SELECT`.

The entire pipeline has been verified live (`docker compose up -d`) and is covered by a Testcontainers
integration test.

---

## 1. Architecture

```text
 Postgres (accounting_db / compliance_db / ledger_db, wal_level=logical)
         │  logical replication (pgoutput)
         ▼
 Kafka Connect (custom image: debezium/connect + the official ClickHouse Kafka Connect Sink plugin)
   ├─ balance-db-connector      → topic cdc.balance.public.JournalEntries      ─┐
   ├─ compliance-db-connector   → topic cdc.compliance.public.ComplianceAudits ─┤ (Debezium source,
   └─ ledger-db-connector       → topic cdc.ledger.public.LedgerEntries       ─┘  SMT: ExtractNewRecordState)
         │
         ▼ (the same Kafka Connect worker, the same topics)
   ├─ clickhouse-sink-balance      (topic2TableMap → analytics.journal_entries)
   ├─ clickhouse-sink-compliance   (topic2TableMap → analytics.compliance_audits)
   └─ clickhouse-sink-ledger       (topic2TableMap → analytics.ledger_entries)
         │  batched, at-least-once, offset committed only after ClickHouse ACKs
         ▼
 ClickHouse (analytics.*: ReplacingMergeTree + non_replicated_deduplication_window)
   └─ analytics.transaction_cube (VIEW, JOIN of three tables by TransactionId)
         │
         ▼
 Metabase (official ClickHouse driver, auto-downloaded at startup)
```

New containers (`docker-compose.yml`): `integration-bus-kafka-connect` (port `8083`, built from
`infrastructure/kafka-connect/Dockerfile` — `debezium/connect:3.0.0.Final` + the official
`ClickHouse/clickhouse-kafka-connect` plugin), `integration-bus-debezium-registrar` (one-shot — registers all 6
connectors via REST API: 3 Debezium source connectors from `infrastructure/debezium/`, 3 ClickHouse sink
connectors from `infrastructure/clickhouse-sink/`), `integration-bus-clickhouse` (`8123` HTTP, `9000` native),
`integration-bus-metabase-driver-setup` (one-shot — downloads `clickhouse.metabase-driver.jar`),
`integration-bus-metabase` (`3001`, mapped to the internal `3000`).

`integration-bus-db` starts with
`command: ["postgres", "-c", "wal_level=logical", "-c", "max_wal_senders=10", "-c", "max_replication_slots=10"]` —
a mandatory requirement for Debezium's logical replication.

---

## 2. ⚠️ Troubleshooting: Version Compatibility and Configuration

The current configuration (`docker-compose.yml`, `infrastructure/clickhouse-sink/*.json`,
`infrastructure/debezium/register-connectors.sh`) already accounts for the compatibility requirements listed
below. This section is a diagnostic reference in case the configuration is changed (e.g. when upgrading an image
version) and one of the symptoms below reappears.

**Kafka broker version.** ClickHouse (via the bundled `librdkafka`) requires a broker no newer than
`apache/kafka:3.9.0` — newer versions (4.x) dropped support for some of the protocol versions `librdkafka` still
uses. If `Local: Required feature not supported by broker` appears in the ClickHouse/Kafka Connect logs, check
the `apache/kafka` image tag in `docker-compose.yml`.

**A ClickHouse password is required for network access.** The official ClickHouse image, without
`CLICKHOUSE_PASSWORD` set, disables network access for the `default` user (container log entry:
`neither CLICKHOUSE_USER nor CLICKHOUSE_PASSWORD is set, disabling network access for user 'default'`) — while
the local `clickhouse-client` inside the container keeps working, which can be misleading during manual
verification. If Kafka Connect or Metabase gets `AUTHENTICATION_FAILED`, make sure `CLICKHOUSE_PASSWORD` is set
in `docker-compose.yml` and that the same password is set in `password` for every sink connector.

**The sink connector's `client_version` must be `V2`.** The `clickhouse-kafka-connect` plugin, with
`client_version=V1` (the default), fails `ping()` with no informative error (`Unable to ping ClickHouse
instance`, instant retries with no stack trace). Each sink connector's configuration in
`infrastructure/clickhouse-sink/*.json` explicitly sets `"client_version": "V2"`.

**Time columns must be `TIMESTAMP WITH TIME ZONE`.** Debezium serializes a Postgres `timestamp` (without a time
zone) as a number (epoch milliseconds), while `timestamptz` is serialized as an ISO-8601 string. Every tracked
column (`JournalEntries.TimestampUtc`, `ComplianceAudits.CreatedAtUtc`, `LedgerEntries.CreatedAt`) is declared as
`timestamp with time zone`, so the sink connector is configured to accept the string format
(`"clickhouseSettings": "date_time_input_format=best_effort"`). If a table added to the pipeline uses a
`timestamp` without a time zone, the ClickHouse parser will fail with `CANNOT_PARSE_INPUT_ASSERTION_FAILED`
without this setting; and if the setting is present but the column still has no time zone, the date will
silently end up in 1970 (the number is interpreted as an epoch value) with no error in the logs whatsoever. Any
new table added to this pipeline must use `timestamp with time zone` and be covered by a regression test, by
analogy with `tests/IntegrationBus.Analytics.Tests`.

**Connector registration survives a slow Kafka Connect REST API startup.** The Kafka Connect worker can respond
`200 OK` to `GET /connectors` before it has fully joined the Connect cluster, which can cause the very first
`POST /connectors` to come back with an empty response. `register-connectors.sh` accounts for this: after the
first successful readiness check, it waits 5 seconds, and each connector registration is retried up to 10 times
at 3-second intervals — this guarantees that `docker compose up -d` reliably provisions the infrastructure in a
single pass on a clean machine.

---

## 3. ClickHouse Data Schema

The full DDL is in `infrastructure/clickhouse/init.sql`. Key tables:

| Table | Engine | Purpose |
|---|---|---|
| `analytics.journal_entries` | ReplacingMergeTree | Balance postings (holds/releases/confirms/top-ups) from `accounting_db.JournalEntries` |
| `analytics.compliance_audits` | ReplacingMergeTree | Compliance-check audit records from `compliance_db.ComplianceAudits` |
| `analytics.ledger_entries` | ReplacingMergeTree | Final ledger postings from `ledger_db.LedgerEntries` |
| `analytics.transaction_cube` | VIEW | JOIN of all three tables by `TransactionId` — a flat slice for reporting |

Every table is declared with `SETTINGS non_replicated_deduplication_window = 100` (a local, non-replicated
analog of block-level deduplication) and stores a raw `__deleted` field (the string `"true"`/`"false"` from
Debezium's `ExtractNewRecordState` SMT), so that a hard delete in Postgres is visible rather than silently
rejected by the sink connector's schema validation. `ORDER BY` is chosen for typical analytical lookups by
account/transaction.

The connectors (`infrastructure/clickhouse-sink/*.json`) use `topic2TableMap` to map a CDC topic to a specific
table, plus a shared set of settings: `client_version=V2`, `exactlyOnce=false` (relying on block-level
deduplication rather than ClickHouse Keeper), `clickhouseSettings=date_time_input_format=best_effort`,
`key.converter.schemas.enable=false`/`value.converter.schemas.enable=false` (these must be set explicitly — the
Kafka Connect worker's defaults lead to the error `JsonConverter with schemas.enable requires "schema" and
"payload" fields`).

---

## 4. How to Verify the Pipeline Is Working

```bash
docker compose up -d
# wait for integration-bus-debezium-registrar and integration-bus-metabase-driver-setup to finish (Exited (0))
docker compose ps

# the status of all 6 connectors (3 Debezium source + 3 ClickHouse sink) should be RUNNING
curl -s "http://localhost:8083/connectors?expand=status"
```

End-to-end check (inserting directly into Postgres — this also works the same way through the real business-flow
API):

```bash
docker exec integration-bus-db psql -U postgres -d ledger_db -c \
  "INSERT INTO \"LedgerEntries\" (\"TransactionId\", \"Amount\", \"CreatedAt\") VALUES (gen_random_uuid(), 777.25, now());"

# a few seconds later:
docker exec integration-bus-clickhouse clickhouse-client --user default --password clickhouse_dev_password --query \
  "SELECT * FROM analytics.ledger_entries ORDER BY Id DESC LIMIT 1 FORMAT Vertical"
```

If the row does not appear, check the specific task's status, and if it is `FAILED`, read the `trace` in the
response:

```bash
curl -s "http://localhost:8083/connectors/clickhouse-sink-ledger/status"
# after fixing the configuration:
curl -s -X POST "http://localhost:8083/connectors/clickhouse-sink-ledger/restart?includeTasks=true"
```

Idempotency (redelivery does not create a duplicate) is verified like this:

```bash
curl -s -X POST "http://localhost:8083/connectors/clickhouse-sink-ledger/restart?includeTasks=true"
docker exec integration-bus-clickhouse clickhouse-client --user default --password clickhouse_dev_password --query \
  "OPTIMIZE TABLE analytics.ledger_entries FINAL"
# count() by TransactionId should remain unchanged
```

---

## 5. Connecting Metabase to ClickHouse

The official open-source Metabase does not bundle a ClickHouse driver by default — `docker-compose.yml`
downloads it itself (`integration-bus-metabase-driver-setup`, the `clickhouse.metabase-driver.jar` file from
`ClickHouse/metabase-clickhouse-driver`) into a shared `/plugins` volume, so no manual driver installation steps
are required.

1. Open `http://localhost:3001` and complete Metabase's initial setup (creating an administrator account — any
   test data works, no email confirmation is required).
2. On the "Add your data" screen (or later: **Settings (gear icon) → Admin settings → Databases → Add
   database**), select **ClickHouse** from the **Database type** dropdown.
3. Fill in the connection fields:
   * **Host**: `integration-bus-clickhouse` (the container name — Metabase reaches it inside the shared Docker
     network).
   * **Port**: `8123`.
   * **Database name**: `analytics`.
   * **Username**: `default`.
   * **Password**: `clickhouse_dev_password` (see §2 above — without a password, `default` has no network
     access).
4. Click **Save**, then **Sync database schema now** — Metabase will pick up all `analytics.*` tables.
5. To build a dashboard: **New → Question → select the `analytics` database** → the `transaction_cube` table (a
   ready-made flat slice) → add the aggregations you need (e.g. the sum of `LedgerAmount` by day) →
   **Visualize** → **Save** → add the question to a new or existing **Dashboard**.

---

## 6. Testing

`tests/IntegrationBus.Analytics.Tests/LedgerCdcPipelineTests.cs` — a `Testcontainers` integration test (Postgres
+ Kafka + Kafka Connect/Debezium+Sink + ClickHouse, all on one Docker network via `Testcontainers.Networks`):
1. Builds the **same custom** Kafka Connect image as `docker-compose.yml`
   (`ImageFromDockerfileBuilder` → `infrastructure/kafka-connect/Dockerfile`), starts every container, creates the
   `LedgerEntries` table (**must** be `TIMESTAMP WITH TIME ZONE`, matching the real EF Core migration — see
   "Time columns must be `TIMESTAMP WITH TIME ZONE`" in §2), registers the same Debezium source connector and
   the same ClickHouse sink connector (`client_version=V2`, `date_time_input_format=best_effort`), and creates the
   same whitelisted ClickHouse table (`ReplacingMergeTree`) as in `infrastructure/clickhouse/init.sql`.
2. Inserts a row into Postgres.
3. Polls ClickHouse until the row appears, via `Policy.Handle<Exception>().WaitAndRetryAsync(...)` (Polly).
4. Verifies **full field-level equality** of the replicated fields (`Amount`, `CreatedAt`) against the original
   insert.

**Important:** the test explicitly pins the `clickhouse/clickhouse-server:24.8` image — the default version used
by `Testcontainers.ClickHouse` (`23.6.3`) is incompatible with the modern Kafka broker, for the same broker
version reason described in §2.
