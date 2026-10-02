# 🚌 integration-bus

A distributed financial transaction platform built around an asynchronous, two-level Saga: a **MassTransit Saga
State Machine** (stateful, cross-service orchestration over Kafka) driving a **MassTransit Courier Routing Slip**
(stateless, local multi-storage orchestration inside the ledger service). The project demonstrates a
**Database-per-Service** topology, real-time CDC analytics, infrastructure-level data masking, and a full
observability stack, all strictly targeting **production-ready** engineering quality in both code and documentation.

> This repository is under active, iterative development. The table below reflects exactly what is implemented and
> verified today; see [`docs/roadmap.md`](docs/roadmap.md) for the complete backlog, including everything still
> planned.

### 📊 Implementation Progress
- [x] **Stage 1: Core Architecture & Async Saga** — ✅ *Done*
- [x] **Stage 2: Observability (Prometheus, Grafana, Loki, Jaeger)** — ✅ *Done* — see [`docs/observability/README.md`](docs/observability/README.md)
- [x] **Stage 3: Reliability & Integration Testing** — ✅ *Done* (distributed locks + rules engine, see [`docs/reliability/README.md`](docs/reliability/README.md))
- [x] **Stage 4: Real-Time Analytics (DWH) & Masking** — ✅ *Done* (Debezium/ClickHouse/Metabase pipeline, see [`docs/data-analytics/README.md`](docs/data-analytics/README.md); infrastructure-level HMAC data masking, see [`docs/reliability/README.md`](docs/reliability/README.md))
- [x] **Stage 5: Cloud-Native Migration (Kubernetes)** — ✅ *Done* (see [`docs/k8s-deployment/README.md`](docs/k8s-deployment/README.md))
- [ ] **Stage 6: High-Load Simulation & Chaos Engineering** — ⏳ *Pending* (strategy documented in [`docs/chaos-engineering/README.md`](docs/chaos-engineering/README.md), not yet executed)
- [ ] **Stage 7: API Gateway Hardening (NGINX, Rate Limiting) & Identity (Keycloak OIDC)** — ⏳ *Pending* (planned, not yet implemented — see [`docs/roadmap.md`](docs/roadmap.md))

### 🔗 Quick Links & Documentation
*   🗺️ **[Project Evolution Roadmap](docs/roadmap.md)** — Detailed task breakdowns, Done criteria, and milestones.
*   🚀 **[API Specifications & Verification Rules](docs/business-logic/api-specifications.md)** — HTTP contracts, JSON payload schemas, FluentValidation constraints, and manual testing procedures.
*   🎯 **[Black-Box Validation Guide](docs/business-logic/validation-guide.md)** — End-to-end scenario proving the whole stack works: HTTP → Saga → Observability → Analytics.
*   ☸️ **[Kubernetes Deployment](docs/k8s-deployment/README.md)** — Full stack as one Helm chart; see [`GETTING-STARTED.md`](docs/k8s-deployment/GETTING-STARTED.md) for a zero-Kubernetes-experience walkthrough.
*   📝 **[Documentation Guidelines](docs/documentation-guidelines.md)** — Strict formatting, language separation, and engineering style rules for human and AI-assisted writing.
*   ⚙️ **[CI Pipeline](.github/workflows/ci.yml)** — GitHub Actions: restore, build (Release), full test run on every push/PR to `main`.
*   📐 **[Git Contribution & Commit Guidelines](CONTRIBUTING.md)** — Semantic commit rules, branching strategy, and issue tracking linkage.

---

## 🎯 Project Overview

The goal is to build a resilient, enterprise-grade distributed financial system using a **Database-per-Service**
architecture, with asynchronous orchestration of distributed transactions as the central engineering challenge —
implemented with **MassTransit Courier (Routing Slips)**, **Apache Kafka**, and (for deployment) **Kubernetes**.

---

## 🧬 Architectural Topology

The system is split into three decoupled operational layers: the core transactional runtime, the real-time
analytics pipeline, and infrastructure-level data masking.

### 1. Core Transactional Runtime (Saga Flow)

```text
                [ External Client ]
                        │
                        ▼
          [ Gateway.Api (YARP HTTP Reverse Proxy) ]
                        │
                        ▼
            [ Processing.Api (REST + Scalar) ]
                        │
                        ▼ (Publish StartTransactionSaga)
                     [ Apache Kafka ]
                        ▲
                        │ (Saga orchestration steps)
         [ SagaOrchestrator.Service — MassTransit Saga State Machine ]
         (EF Core Transactional Outbox + Consumer Inbox)
                        │
       ┌────────────────┼────────────────────────────────┐
       ▼ (Step 1)       ▼ (Step 2)                       ▼ (Step 3 — Courier Routing Slip)
[ AccountBalance.Service ]  [ Compliance.Service ]   [ CoreLedger.Service ]
  Redis Distributed Lock      Declarative RulesEngine   WriteAuditTrail → UpdateCache →
  (RedLock.net) +              (JSON-configured limits)  PublishLedgerCommitted
  event-sourced journal                                  (automatic technical rollback
  + Postgres                  + Postgres                 on late-stage failure)
                                                          + Postgres + Redis
```

*Storage model:* `AccountBalance.Service` uses an **Event Sourcing** model — every balance mutation is an
append-only journal entry (hold / release / confirm / top-up), with periodic snapshots for fast state
reconstruction, eliminating row-lock contention under concurrent writes to the same account.

### 2. Real-Time Analytics Pipeline (CDC)

```text
 Postgres (accounting_db / compliance_db / ledger_db, wal_level=logical)
         │ logical replication (pgoutput)
         ▼
 Kafka Connect (Debezium source connectors + official ClickHouse Kafka Connect Sink)
         │
         ▼ (at-least-once, batched, offset committed only after ClickHouse ACK)
 ClickHouse (ReplacingMergeTree tables + transaction_cube view)
         │
         ▼
 Metabase dashboards
```

See [`docs/data-analytics/README.md`](docs/data-analytics/README.md) for the full pipeline design.

### 3. Infrastructure-Level Data Masking

A generic MassTransit consume filter inspects every message for `[SensitiveData]`-attributed properties, mirrors an
HMAC-SHA256-masked shadow copy onto a dedicated `{topic}.security` Kafka topic, and forwards the untouched original
message to the real consumer unchanged. See [`docs/reliability/README.md`](docs/reliability/README.md).

---

## 🛠️ Technology Stack

*   **Runtime:** `.NET 10`, C# 13.
*   **API Layer:** `YARP` (Yet Another Reverse Proxy) as a pure HTTP gateway + `ASP.NET Core` Web API with `Asp.Versioning` and `Scalar` interactive API docs.
*   **Message Broker & Async Transport:** `Apache Kafka` + `MassTransit` (Kafka Rider, Saga State Machine, Courier Routing Slip, EF Core Transactional Outbox/Inbox).
*   **Databases (OLTP):** `PostgreSQL` (isolated database per service) + `Redis` (distributed locks via `RedLock.net`).
*   **Reliability:** Declarative compliance rules via `RulesEngine` (JSON-configured, hot-swappable without code changes).
*   **Data Pipelines & Streaming (CDC):** `Debezium` (Kafka Connect source connector) + the official ClickHouse Kafka Connect Sink connector.
*   **Analytics & DWH (OLAP):** `ClickHouse` (`ReplacingMergeTree` tables + a flat `transaction_cube` view) + `Metabase` for dashboards.
*   **Observability:** `OpenTelemetry` (traces + metrics) + `Prometheus` + `Grafana` + `Jaeger` (distributed tracing) + `Loki`/`Serilog` (centralized structured logs, trace-correlated).
*   **Orchestration & Infrastructure:** `Docker Compose` for local development; `Kubernetes` (Kind/K3s) + `Helm` as an additional, fully equivalent deployment path.
*   **Testing:** `xUnit`, `FluentAssertions`, `NSubstitute`, `Testcontainers` (Postgres, Redis, Kafka, ClickHouse), `GitHub Actions` CI.

---

## ⚙️ Distributed Saga & Routing Slip Design

The system implements the **Saga Orchestration** pattern using **MassTransit**. A transaction is executed as a
sequence of commands/events over dedicated Kafka topics, coordinated by a persisted state machine.

### The financial transfer saga:
1.  **Hold** (`AccountBalance.Service`): appends an immutable negative-delta hold entry to the event-sourced journal, guarded by a Redis distributed lock on the source account.
    - *Compensate:* appends a positive neutralizing entry, restoring available capacity.
2.  **Compliance Check** (`Compliance.Service`): evaluates the transaction against declarative JSON rules via `RulesEngine`.
    - *Compensate (on rejection):* triggers release of the balance hold.
3.  **Ledger Commit** (`CoreLedger.Service`): executes a local Courier Routing Slip — writes the audit trail, updates the Redis read cache, and publishes the committal notification — with automatic technical rollback across all three steps if a late activity fails.
4.  **Confirm** (`AccountBalance.Service`): finalizes the double-entry journal confirmation.
    - *Compensate (on failure):* triggers release of the balance hold.

Every consumer is idempotent under Kafka's at-least-once redelivery, and the orchestrator's EF Core Transactional
Outbox/Inbox guarantees that a broker outage mid-saga rolls back the state transition atomically rather than
leaving a partially-advanced instance.

---

## 🚀 Running Locally

### Prerequisites
* Docker Desktop (or an equivalent Docker Engine + Compose installation).

### Docker Compose (primary path)
```bash
git clone <this-repository>
cd integration-bus
docker compose up -d --build
```
Follow [`docs/business-logic/validation-guide.md`](docs/business-logic/validation-guide.md) for a full black-box
walkthrough (seed accounts → run a transaction → verify the saga, observability stack, and analytics pipeline).

### Kubernetes (additional, fully equivalent path)
```bash
kind create cluster --config deploy/k8s/kind/kind-cluster.yaml
docker compose build
helm install integration-bus deploy/k8s/charts/integration-bus -n integration-bus --create-namespace --timeout 10m
```
See [`docs/k8s-deployment/README.md`](docs/k8s-deployment/README.md) for the full chart walkthrough, or
[`docs/k8s-deployment/GETTING-STARTED.md`](docs/k8s-deployment/GETTING-STARTED.md) if you have never used Kubernetes before.

### 🛠️ Development
Before making any changes or submitting Pull Requests, please review [`CONTRIBUTING.md`](CONTRIBUTING.md).
