# 🚌 integration-bus

> **⚠️ Project Status: In Active Development (Stage 1 / MVP Skeleton)**  
> This repository represents a live, step-by-step architectural evolution. Features documented below are being rolled out incrementally according to the project roadmap.

### 📊 Implementation Progress
- [ ] **Stage 1: Core Architecture & Async Saga** — 🔄 *In Progress (Working on Issue #2)*
- [ ] **Stage 2: Reliability & Integration Testing** — ⏳ *Pending*
- [ ] **Stage 3: Real-Time Analytics (DWH) & Masking** — ⏳ *Pending*
- [ ] **Stage 4: Cloud-Native Migration (Kubernetes)** — ⏳ *Pending*
- [ ] **Stage 5: High-Load Simulation & Chaos Engineering** — ⏳ *Pending*

---

## 🎯 Project Overview
This repository serves as a practical, production-ready blueprint for **Platform Engineering** and **Advanced Cloud-Native System Design**. The goal is to build a highly resilient, enterprise-grade distributed financial system using a **Database-per-Service** architecture, with zero complex business logic under the hood.

Instead of reinventing the wheel, this project focuses on high-load infrastructure integration, chaos engineering, real-time data streaming (CDC), and asynchronous orchestration of distributed transactions using **MassTransit Courier (Routing Slips)**, **Apache Kafka**, and **Kubernetes**.

---

## 🧬 Architectural Topology

The system topology is split into three decoupled operational layers: Core Transactional Runtime, Real-Time Analytics Pipeline, and Secure Data Anonymization.

### 1. Core Transactional Runtime (Saga Flow)

This layer handles the lifecycle of synchronous incoming requests and orchestrates the distributed financial transaction across microservices using isolated databases (Database-per-Service).

```text
       [ External Client / k6 Load Test ]
                       │
                       ▼
         [ NGINX (SSL / Rate Limiting) ]
                       │
                       ▼
       [ API Gateway (YARP HTTP Proxy) ]
                       │
       ┌───────────────┴───────────────┐
       ▼ (gRPC Token Validation)       ▼ (HTTP Forwarding)
[ Keycloak Auth ]                   [ WebAPI Processing Service ]
                                       │
                                       ▼ (Start Distributed Saga)
                                    [ Apache Kafka (Events Broker) ]
                                       ▲
                                       │ (Saga Orchestration Steps Flow)
                                    [ MassTransit / Stateful Orchestrator ]
                                       │
       ┌───────────────────────────────┼───────────────────────────────┐
       ▼ (Step 1)                      ▼ (Step 2)                      ▼ (Step 3)
[ Account Balance Service ]     [ Compliance Service ]          [ Core Ledger Service ]
  └─► [ Redis + Postgres DB ]     └─► [ Postgres Comp DB ]        └─► [ Postgres Ledger DB ]
```

### 2. Real-Time Analytical Contour (OLAP)

*Note: This pipeline is executed symmetrically for all three transactional databases (`Balance DB`, `Compliance DB`, and `Ledger DB`). The diagram below illustrates the flow for a single database instance.*

```text
 [ Postgres App DB ] (One of: Balance / Compliance / Ledger DB)
         │
         │ (Incremental Pull via IAsyncEnumerator)
         ▼
 ┌───────────────────────────────────────────────────────────────────┐
 │               [ Custom .NET 9 ETL Ingestion Worker ]              │
 │ - High-Watermark checkpoint tracking (WHERE id > max_id)          │
 │ - In-Memory Batching & Schema Validation (System.Threading)       │
 └─────────────────────────────────┬─────────────────────────────────┘
                                   │
                                   ▼ (Bulk Inserts in Patches)
                       [ ClickHouse OLAP Cubes ]
                                   │
                                   ▼
                     [ Metabase Dashboard Reports ]
```

### 3. Secure Data Anonymization Pipeline (Prod-to-Test)

*Note: To protect production PII and banking secrets, this CDC pipeline replicates all transactional databases into mirrored, fully obfuscated environments for development and QA teams.*

```text
 [ Postgres Prod DB ] (One of: Balance / Compliance / Ledger DB)
         │
         │ (Asynchronous WAL Log Capture with zero OLTP CPU overhead)
         ▼
 ┌───────────────────────────────────────────────────────────────────┐
 │                     [ Debezium CDC Cluster ]                      │
 │ - Single Message Transformations (SMT) for on-the-fly masking     │
 │ - Deterministic salted hashing of Account IDs and PII data        │
 └─────────────────────────────────┬─────────────────────────────────┘
                                   │
                                   ▼ (Masked Obfuscated Stream)
                       [ Kafka Security Topics ]
                                   │
                                   ▼ (Row-by-Row Ingestion)
                     [ Isolated PostgreSQL Test DB ] 
                       (balance_test / compliance_test / ledger_test)
```

---

## 🛠️ Technology Stack

*   **Orchestration & Infrastructure:** `Kubernetes` (K3s / Kind) + `Helm` for cloud-native deployment.
*   **Reverse Proxy & Gateway:** `NGINX` (Rate Limiting, Header Sanitization) + `YARP (Yet Another Reverse Proxy)` with built-in Keycloak JWT validation.
*   **Identity & Access Management:** `Keycloak` (OAuth2/OIDC Auth Server).
*   **Message Broker & Async Transport:** `Apache Kafka` + `MassTransit` (Courier / Routing Slip engine).
*   **Databases (OLTP):** `PostgreSQL` (Isolated databases for each service) + `Redis` (Distributed Locks via RedLock.net & Idempotency Storage).
*   **Data Pipelines & Streaming (CDC):** `Debezium CDC` (Streaming WAL-logs to Kafka) for decoupled data synchronization.
*   **Analytics & DWH (OLAP):** `ClickHouse` (Pre-calculated OLAP Cubes via `Materialized Views` & `SummingMergeTree`) + `Metabase` for real-time dashboards.
*   **Observability:** `OpenTelemetry` + `Prometheus` + `Grafana` + `Jaeger` (Distributed Tracing across all microservices).

---

## 🪵 Business Logic & Minimal Context
To avoid getting bogged down in domain engineering, the business logic is stripped down to a bare minimum financial context:
*   **Endpoint:** `POST /api/ledger/transaction`
*   **Payload:** `{ "TransactionId": "GUID", "From": "UUID", "To": "UUID", "Amount": 100 }`
*   **Processing:** The gateway accepts the payload, validates it via `FluentValidation`, and fires a Distributed Saga. Each microservice contains primitive `if/else` checks acting as infrastructure execution mocks.

---

## ⚙️ Asynchronous Sagas & Routing Slips Design

The system implements the **Saga Orchestration** pattern using **MassTransit Courier**. Transactions are executed as an immutable series of activities forwarded through dedicated Kafka topics.

### The Financial Transfer Saga Steps:
1.  **`HoldMoneyActivity`** (`Account Balance Service`): Deducts the amount from the sender's balance using Redis distributed locks.
    *   *Compensate:* Returns the money if subsequent steps fail (`Balance += Amount`).
2.  **`ComplianceCheckActivity`** (`Compliance Service`): Evaluates transaction limits and risk levels using declarative `RulesEngine` via a local JSON config.
    *   *Compensate:* No-op (logs compliance security alert).
3.  **`CommitLedgerActivity`** (`Core Ledger Service`): Writes the immutable audit trail record into the core database.

---

## 📊 Real-Time Analytics & Data Masking (OLAP / DWH)

To prevent heavy analytical queries from locking the production transactional databases, the project demonstrates advanced **OLAP Data Warehousing**:
*   **Zero-Overhead ETL:** `Debezium` captures raw data changes directly from PostgreSQL WAL logs asynchronously, putting zero CPU load on OLTP instances compared to cron-based batching.
*   **Modern ROLAP Cubes:** `ClickHouse` aggregates raw event streams from different microservice schemas on the fly into flat, high-performance analytical tables using `Materialized Views`.
*   **Data Anonymization (Prod-to-Test):** A data pipeline masks sensitive production data (names, balances, contacts) deterministically using salted hashes, creating a safe, compliance-friendly copy for the `PostgreSQL Test DB`.

---

## 🌋 Chaos Engineering & Resiliency Test Cases

The primary technical challenge is to simulate severe infrastructure outages in a Kubernetes cluster and observe how the system preserves data consistency.

### Tested Scenarios:
*   **Pod Eviction & Failover:** Abruptly deleting microservice pods using `kubectl` mid-transaction. Testing how `Polly` policies handle transient network blips and how Kafka manages consumer group rebalancing.
*   **Split-Brain & Distributed Lock Break:** Injecting network latency between services and Redis. Validating how PostgreSQL `UNIQUE CONSTRAINTS` act as the ultimate line of defense against duplicate saga executions (**Exactly-Once delivery**).
*   **Saga Compensation Verification:** Artificially triggering a validation failure on Step 2 (`Compliance Service`) and verifying that MassTransit automatically rolls back the balance in Step 1 without data corruption.

---

## ⚙️ Infrastructure & Local Environment Setup

### 1. Database Initialization
Ensure the centralized PostgreSQL container is fully initialized. The baseline schema footprints are established inside the logical partitions within the `integration-bus-db` container container instance.

### 2. Manual Apache Kafka Topics Provisioning
To align with high-performance production constraints and maintain boundary safety, automatic topic creation is disabled on the broker. You must provision the following required topics manually before spinning up the backend services.

Open **Kafka UI** at `http://localhost:8080`, navigate to the **Topics** section, click **Add a Topic**, and create the following entities utilizing a baseline layout (1 Partition, Replication Factor 1):

* `saga-transaction-start` — Ingests initialization trigger commands dispatched from the API layer.
* `account-balance-hold` — Dispatched by the orchestrator to request asset locks inside the Balance context.
* `account-balance-hold-passed` — Callback event signaling absolute success from the Balance worker.
* `account-balance-hold-failed` — Callback event signaling validation or technical bounds violations from the Balance worker.
* `compliance-limits-check` — Dispatched to trigger regulatory velocity verification steps.
* `core-ledger-record-write` — Dispatched to trigger the immutable financial ledger audit trailing records.

---

## 🚀 How to Run Locally

### Prerequisites
* Docker Desktop / Rancher Desktop
* Kubernetes cluster enabled (`kubectl`)
* Helm installed

### Installation Steps
1. Clone the repository.
2. Deploy the infrastructure stack inside Kubernetes:
   ```bash
   helm install integration-infra ./deploy/k8s/charts/infra
3. Run load tests via `k6` to monitor system performance and view real-time metrics in Grafana and Metabase dashboards:
   ```bash
   k6 run ./tests/load-test.js
   ```

### 🛠️ Development
Before making any changes or submitting Pull Requests, please review our [Commit Guidelines](CONTRIBUTING.md).
