# 🧪 Kafka Consumers: Manual Infrastructure Validation

This document provides JSON payloads and instructions for manually validating MassTransit's Kafka deserialization
pipeline, consumer wiring, and telemetry for each worker service — useful when developing or debugging a single
service in isolation, without driving a full saga through the HTTP API.

---

## 🚀 Prerequisites

1. The infrastructure stack is running (`docker compose up -d`): `integration-bus-kafka`, `integration-bus-db`, and
   `integration-bus-redis` are healthy.
2. The target `.NET 10` worker service is running (via `docker compose up -d <service>` or from your IDE).
3. Open **Kafka UI** at `http://localhost:8080`.
4. Navigate to **Topics**, select the target topic, click **Produce Message**, and paste the JSON payload below.
5. Observe the worker's console logs (or Loki/Grafana — see [`docs/observability/README.md`](../observability/README.md)) for the expected log line.

---

## 📦 Per-Service Validation Payloads

### 0. Saga Orchestrator
* **Topic:** `saga-transaction-start`
* **Worker:** `IntegrationBus.SagaOrchestrator.Service`
* **Expected Log:** `Saga step 1/4 | Dispatching HoldAccountBalance for Tx: {TransactionId}, ...`

```json
{
  "transactionId": "b1111111-2222-3333-4444-555555555555",
  "sourceAccountId": "a2222222-3333-4444-5555-999999999999",
  "targetAccountId": "c3333333-4444-5555-7777-777777777777",
  "amount": 1500.00,
  "currency": 1
}
```

### 1. Account Balance Service
* **Topic:** `account-balance-hold`
* **Worker:** `IntegrationBus.AccountBalance.Service`
* **Expected Log:** `Processing event-sourced balance hold for Tx: {TransactionId}, Source Account: {SourceAccountId}, Target Account: {TargetAccountId}`

```json
{
  "transactionId": "b1111111-2222-3333-4444-555555555555",
  "accountFromId": "a2222222-3333-4444-5555-999999999999",
  "accountToId": "c3333333-4444-5555-7777-777777777777",
  "amount": 1500.00,
  "currency": 1
}
```

### 2. Compliance Service
* **Topic:** `compliance-limits-check`
* **Worker:** `IntegrationBus.Compliance.Service`
* **Expected Log:** `Successfully persisted and dispatched compliance passing event for TransactionId: {TransactionId}` (or a `...Failed` event if the message violates one of the declarative rules in `Rules/compliance-rules.json` — see [`docs/reliability/README.md`](../reliability/README.md) §4).

```json
{
  "transactionId": "b1111111-2222-3333-4444-555555555555",
  "sourceAccountId": "a2222222-3333-4444-5555-999999999999",
  "targetAccountId": "c3333333-4444-5555-7777-777777777777",
  "amount": 1500.00,
  "currency": 1
}
```

### 3. Core Ledger Service
* **Topic:** `core-ledger-record-write`
* **Worker:** `IntegrationBus.CoreLedger.Service`
* **Expected Log:** `Ingesting external ledger command for TransactionId: {TransactionId}. Building local technical routing slip context.`

```json
{
  "transactionId": "b1111111-2222-3333-4444-555555555555",
  "sourceAccountId": "a2222222-3333-4444-5555-999999999999",
  "targetAccountId": "c3333333-4444-5555-7777-777777777777",
  "amount": 1500.00,
  "currency": 1
}
```

---

## Distributed Transaction Flow & Compensation Lifecycle

The system uses two levels of orchestration: a **global stateful Saga** (MassTransit Saga State Machine over
Kafka) and a **local stateless Courier Routing Slip** (inside `CoreLedger.Service`, over an in-memory bus) for the
ledger-commit step.

```text
[Hold] ──> [Compliance Check] ──> [Ledger Commit (Routing Slip)] ──> [Confirm]
   │               │                        │                          │
   └── terminal    └── release hold         └── local technical        └── release hold
       (no               (global                  rollback across          (global
       compensation)     compensation)            WriteAuditTrail/         compensation)
                                                    UpdateCache/
                                                    PublishLedgerCommitted
```

---

## 🏁 Expected Healthy State

* The MassTransit bus for each service starts without throwing a `KafkaConnectionException`.
* Each consumer's handler executes and produces the corresponding `...Passed`/`...Failed` outcome event.
* No `SerializationException` appears in any consumer's execution pipeline.

For a full end-to-end run driven through the real HTTP API rather than manual topic production, see
[`validation-guide.md`](validation-guide.md).
