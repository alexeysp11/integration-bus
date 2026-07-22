# 🧪 Kafka Consumers Infrastructure Validation Guide

This document provides deterministic JSON payloads and instructions required to validate the message-conduction capacity, MassTransit deserialization pipelines, and Serilog telemetry configurations across all isolated backend worker service skeletons.

---

## 🚀 Pre-requisites & Verification Flow

1. Ensure the centralized Docker environment (`integration-bus-kafka`, `integration-bus-db`) is fully operational.
2. Launch the target `.NET 9 Worker` service within your IDE or via the terminal.
3. Access the **Kafka UI** management dashboard at `http://localhost:8080`.
4. Navigate to the **Topics** section, locate the target topic, click **Produce Message**, and dispatch the corresponding JSON payload specified below.
5. Verify the execution boundaries by auditing the worker's console logs for the simulated database transactional outputs.

---

## 📦 Service Verification Payloads

### 0. Saga Orchestrator Context
* **Target Topic:** `saga-transaction-start`
* **Target Worker:** `IntegrationBus.SagaOrchestrator`
* **Expected Telemetry Output:** MassTransit logs indicating the creation of a new Saga State instance and an outbound dispatch trigger toward the `account-balance-hold` endpoint.

```json
{
  "transactionId": "b1111111-2222-3333-4444-555555555555",
  "sourceAccountId": "a2222222-3333-4444-5555-999999999999",
  "targetAccountId": "c3333333-4444-5555-7777-777777777777",
  "amount": 1500.00,
  "currency": 1
}
```

### 1. Account Balance Service Context
* **Target Topic:** `account-balance-hold`
* **Target Worker:** `IntegrationBus.AccountBalance.Service`
* **Expected Telemetry Output:** `[INFO] [HoldAccountBalanceConsumer] Database: balance | Simulating SQL write: INSERT INTO AccountHolds...`

```json
{
  "transactionId": "b1111111-2222-3333-4444-555555555555",
  "accountId": "a2222222-3333-4444-5555-999999999999",
  "amount": 1500.00
}
```

### 2. Compliance Service Context
* **Target Topic:** `compliance-limits-check`
* **Target Worker:** `IntegrationBus.Compliance.Service`
* **Expected Telemetry Output:** `[INFO] [CheckComplianceLimitsConsumer] Database: compliance | Executing SQL: INSERT INTO ComplianceAudit...`

```json
{
  "transactionId": "b1111111-2222-3333-4444-555555555555",
  "sourceAccountId": "a2222222-3333-4444-5555-999999999999",
  "targetAccountId": "c3333333-4444-5555-7777-777777777777",
  "amount": 1500.00,
  "currency": 1
}
```

### 3. Core Ledger Service Context
* **Target Topic:** `core-ledger-record-write`
* **Target Worker:** `IntegrationBus.CoreLedger.Service`
* **Expected Telemetry Output:** `[INFO] [WriteLedgerRecordConsumer] Database: ledger | Executing SQL: INSERT INTO LedgerEntries...`

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

## Distributed Transaction Flow & Multi-Level Compensation Lifecycle

The system utilizes a two-level orchestration engine: **Global Stateful Orchestration** (via MassTransit Saga State Machine and Apache Kafka) and **Local Stateless Orchestration** (via MassTransit Courier Routing Slips over InMemory bus inside the Ledger domain).

```text
[Step 1: Ingestion API] ──> [Step 2: Account Balance] ──> [Step 3: Compliance] ──> [Step 4: Core Ledger (Routing Slip)]
                                  │                            │                        │
Compensations:                    └── None (Terminal Failure)  └── Global Release       └── 1. Local Automated Compensation (Courier)
                                                                                            2. Global Balance Release Trigger
```

---

## 🏁 Definition of Done Criteria Check

The infrastructure footprint validation is considered absolute if:
* The MassTransit bus initializes successfully without shying away with a `KafkaConnectionException`.
* The target consumer internal state successfully triggers upon message ingestion.
* No `SerializationException` errors are registered inside the consumer execution pipelines.
