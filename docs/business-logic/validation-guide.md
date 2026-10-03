# 🎯 Validation Guide: End-to-End "Black Box" Verification Scenario

[English](validation-guide.md) | [Русский](validation-guide.ru.md)

A step-by-step scenario for an engineer running the system for the first time, to confirm that the whole stack —
from an HTTP request to the analytics dashboard in ClickHouse — works end to end. All commands are `bash`/`cURL`,
with no prior knowledge of the internal code required.

---

## Step 1. Start the System and Check Its Health

```bash
cd integration-bus
docker compose up -d
```

Wait for the one-shot init containers to finish (`Exited (0)`); the rest should be in the `Up` state:

```bash
docker compose ps
```

Check the health endpoints of all six .NET services (each should return `200 OK` / `Healthy`):

```bash
curl -s -o /dev/null -w "Gateway: %{http_code}\n"            http://localhost:5038/health
curl -s -o /dev/null -w "Processing.Api: %{http_code}\n"     http://localhost:5201/health
curl -s -o /dev/null -w "SagaOrchestrator: %{http_code}\n"   http://localhost:6001/health
curl -s -o /dev/null -w "AccountBalance: %{http_code}\n"     http://localhost:6002/health
curl -s -o /dev/null -w "Compliance: %{http_code}\n"         http://localhost:6003/health
curl -s -o /dev/null -w "CoreLedger: %{http_code}\n"         http://localhost:6004/health
```

If any service is still `Unhealthy` — wait 10–20 seconds (database migrations and Kafka topic provisioning run on
first startup) and retry. The status of all 6 Kafka Connect connectors (3 Debezium source + 3 ClickHouse sink)
should be `RUNNING`:

```bash
curl -s "http://localhost:8083/connectors?expand=status"
```

---

## Step 2. Prepare Test Accounts

The system has no separate public "create one account" endpoint — accounts are only created via bulk seeding.
Let's create 5 test accounts in USD and grab two real `Id`s from the database:

```bash
curl -s -X POST http://localhost:5038/api/v1/accounts/seed \
  -H "Content-Type: application/json" \
  -d '{"count": 5, "currency": 1}'
# -> 202 Accepted

sleep 2

docker exec integration-bus-db psql -U postgres -d accounting_db -t -c \
  "SELECT \"Id\" FROM \"Accounts\" ORDER BY \"CreatedAt\" DESC LIMIT 2;"
```

Save two GUIDs from the output as `SOURCE_ID` and `TARGET_ID`. Top up the source account's balance so there is
enough to transfer (freshly seeded accounts have a balance of 0):

```bash
curl -s -X POST "http://localhost:5038/api/v1/accounts/$SOURCE_ID/topup" \
  -H "Content-Type: application/json" \
  -d '{"amount": 1000.00, "currency": 1}'
# -> 202 Accepted {"message":"Top-up request accepted...","trackingTransactionId":"..."}

sleep 2
```

---

## Step 3. Send a Test Transaction

**Endpoint:** `POST http://localhost:5038/api/v1/ledger/transaction`
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

### Expected Result

**Status code:** `202 Accepted`

**Response body:**
```json
{
  "transactionId": "b1111111-2222-3333-4444-999999999977",
  "status": "Processing",
  "message": "Your transaction payload has been accepted and queued for processing."
}
```

Save the `transactionId` from the response — you will need it in the following steps.

> **There is no separate GET endpoint for polling saga status.** To see the final state of the distributed
> transaction as a black box, query the saga state table directly:

```bash
sleep 3
docker exec integration-bus-db psql -U postgres -d saga_db -c \
  "SELECT \"CorrelationId\", \"CurrentState\", \"ErrorMessage\" FROM \"TransactionState\" WHERE \"CorrelationId\" = '<transactionId>';"
```

`CurrentState = Completed` and `ErrorMessage = NULL` are expected — the saga completed all 4 steps
(Hold → Compliance → Ledger → Confirm) successfully.

---

## Step 4. Verify the Observability Stack

1. **Loki (logs).** Open Grafana → `http://localhost:3000` (login/password `admin`/`admin`) → **Explore** →
   select the **Loki** datasource (configuration — [`docs/observability/README.md`](../observability/README.md)
   §3.2) → query:
   ```logql
   {service_name="integration-bus-saga-orchestrator-service"} |= "<transactionId>"
   ```
   The matching log lines will contain a `TraceId` field (added by `Serilog.Enrichers.Span`).

2. **Jaeger (tracing).** Copy the `TraceId` from the log and open:
   ```text
   http://localhost:16686/trace/<TraceId>
   ```
   A waterfall diagram should appear with spans from at least `integration-bus-processing-api` and
   `integration-bus-saga-orchestrator-service` — the message's end-to-end path through the saga's Kafka topics.

3. **Prometheus (metrics).** Open `http://localhost:9090/graph` and run the query:
   ```promql
   rate(http_server_request_duration_seconds_count{job="processing-api"}[5m])
   ```
   The graph should show a non-zero spike at the moment the request was sent in Step 3 (the exact set of
   exported metrics is documented in
   [`docs/observability/README.md`](../observability/README.md) §4.2; Prometheus's autocomplete will suggest the
   exact names).

---

## Step 5. Verify the Analytics Pipeline (ClickHouse)

A few seconds after the saga completes (Debezium → Kafka → ClickHouse Sink Connector, see
[`docs/data-analytics/README.md`](../data-analytics/README.md)), the changed data should appear in ClickHouse:

```bash
docker exec integration-bus-clickhouse clickhouse-client \
  --user default --password clickhouse_dev_password --query \
  "SELECT * FROM analytics.transaction_cube WHERE TransactionId = '<transactionId>' FORMAT Vertical"
```

The expected result is one row with:
- `JournalEntryType = 1` (Hold) and `JournalAmountDelta = -100` (100.00 debited from the source account);
- `ComplianceStatus = 2` (Passed);
- `LedgerAmount = 100` and a non-empty `LedgerCommittedAt`.

If the row did not appear — check the sink connectors' status
(`curl -s http://localhost:8083/connectors/clickhouse-sink-ledger/status`) and the "How to verify the pipeline is
working" section in [`docs/data-analytics/README.md`](../data-analytics/README.md) §4.

---

## Summary

If all 5 steps succeeded — you have verified the entire system live: the HTTP layer (Gateway → Processing.Api),
the distributed saga (SagaOrchestrator → AccountBalance → Compliance → CoreLedger) with distributed locking and
declarative rules, infrastructure-level HMAC masking (written in parallel to `*.security` topics, see
[`docs/reliability/README.md`](../reliability/README.md)), the observability stack (Prometheus/Loki/Jaeger), and
the CDC analytics pipeline (Debezium → Kafka Connect ClickHouse Sink → ClickHouse → Metabase).
