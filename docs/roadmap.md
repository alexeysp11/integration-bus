# 🗺️ Project Roadmap & Backlog: integration-bus

This document outlines the complete iterative implementation plan for the `integration-bus` platform, broken down into 5 sequential stages and 9 actionable issues.

---

## 🚀 Stage 1: Core .NET Architecture & Asynchronous Saga (MVP)

### 📌 Issue #1: Base Infrastructure and Gateway Skeleton
*   **Git Branch:** `feature/issue-1`
*   **Description:** Establish the foundational infrastructure and service layouts for the `integration-bus` platform. The goal is to spin up the core message broker, database, and cache containers via Docker, initialize a pure API Gateway using YARP (Yet Another Reverse Proxy) for request forwarding, and deploy the baseline `ProcessingService` skeleton with verified MassTransit/Kafka connectivity on startup.
*   **Todo List:**
    - [x] Create the root repository directory structure (`src/`, `tests/`, `docs/`, `infrastructure/`).
    - [x] Spin up the local infrastructure stack (`kafka`, `kafka-ui`, `redis`, `postgres`) using `docker-compose.yml` (without physical volumes for clean test environments).
    - [x] Create an initialization script (`infrastructure/postgres/init.sql`) to provision isolated databases for all future microservices (`gateway`, `balance`, `compliance`, `ledger`).
    - [x] Initialize the `IntegrationBus.Gateway.Api` project using .NET 9 and configure it as a lightweight YARP reverse proxy redirecting `/api/ledger/**` traffic to the processing backend.
    - [x] Initialize the `IntegrationBus.Processing.Api` project (.NET 9 Web API) configured on port `5201`.
    - [x] Install open-source Apache-2.0 licensed MassTransit packages (`v8.5.10`) into `ProcessingService` and establish a startup connection to the local Kafka broker (`localhost:9092`).
*   **Definition of Done:**
    - Infrastructure stack starts smoothly with `docker compose up -d` and database footprints are automatically initialized.
    - `IntegrationBus.Gateway.Api` compiles, boots up, and successfully forwards a dummy HTTP POST request to `IntegrationBus.Processing.Api`.
    - `IntegrationBus.Processing.Api` successfully validates its connection to the Kafka broker on startup without throwing exceptions or crashing.

### 📌 Issue #2: Project Skeletons for Saga Participants
*   **Git Branch:** `feature/issue-2`
*   **Description:** Following the **Database-per-Service** architectural pattern, isolate the execution environments for each distributed transaction step. This task involves establishing three backend worker services (Balance, Compliance, Ledger) and initializing a central standalone `IntegrationBus.SagaOrchestrator` worker that will drive the stateful transaction machine.
*   **Todo List:**
    - [ ] Initialize `IntegrationBus.SagaOrchestrator` (.NET 9 Worker) responsible for handling MassTransit Saga State Machine logic.
    - [ ] Initialize `IntegrationBus.AccountBalance.Service` (.NET 9 Worker) to process transaction funds reservation.
    - [ ] Initialize `IntegrationBus.Compliance.Service` (.NET 9 Worker) to process declarative limit checks.
    - [ ] Initialize `IntegrationBus.CoreLedger.Service` (.NET 9 Worker) to write the immutable final transaction records.
    - [ ] Install MassTransit `v8.5.10` in all 4 new projects and configure baseline consumers subscribing to their respective Kafka topics.
*   **Definition of Done:**
    - Dedicated directories, solutions, and internal dependency injection footprints are created for the orchestrator and all three saga steps.
    - Each service runs independently as a standalone host and references only its dedicated infrastructure/database connections.

### 📌 Issue #3: Asynchronous Saga Orchestration via MassTransit Courier
*   **Git Branch:** `feature/issue-3`
*   **Description:** Implement the complete end-to-end asynchronous distributed transaction lifecycle using **MassTransit Saga State Machine** (Stateful Orchestration). The `ProcessingService` must ingest HTTP requests and fire a startup trigger. The `SagaOrchestrator` must coordinate step-by-step executions across the network. Additionally, add a transaction lookup mechanism for client status polling.
*   **Todo List:**
    - [ ] Implement a stateful Saga State Machine class within `SagaOrchestrator` to track financial execution states (`Started`, `FundsHeld`, `ComplianceApproved`, `Completed`, `Failed`).
    - [ ] Code the state consumers: `HoldMoneyCommand` (with rollback compensation), `CheckComplianceCommand`, and `CommitLedgerCommand`.
    - [ ] Implement a nested **MassTransit Courier Routing Slip** inside `CoreLedger.Service` to handle `CommitLedgerCommand` via three sequential technical activities: `WriteAuditTrailActivity` (Postgres), `UpdateCacheActivity` (Redis), and `InvalidateDwhWatermarkActivity` (ETL marker).
    - [ ] Update `IntegrationBus.Processing.Api` to publish the initial `StartTransactionCommand` to Kafka and immediately return `HTTP 202 Accepted` along with a unique `TransactionId` (GUID) to the caller.
    - [ ] Implement an explicit polling endpoint `GET /api/transactions/{id}` inside `ProcessingService` that reads the current execution state from the database, allowing clients to track saga outcomes.
*   **Definition of Done:**
    - Postman sending a POST transaction receives a superfast `202 Accepted` reply.
    - The distributed transaction executes flawlessly in the background across Kafka topics (Balance -> Compliance -> Ledger).
    - The Ledger service successfully executes its internal technical steps via a `Routing Slip`; if a late activity fails (e.g., Redis timeout), it triggers automated local compensations.
    - Making a GET request to the polling endpoint correctly reflects the completed financial or compensated failure state of the transaction.

---

## 🧪 Stage 2: Reliability Engineering & Integration Testing

### 📌 Issue #4: Integration Testing for Saga Compensations
*   **Git Branch:** `test/issue-4`
*   **Description:** Add a comprehensive integration test suite using WebApplicationFactory and the MassTransit test harness to ensure that infrastructure and business validation errors trigger the correct automated rollback behaviors.
*   **Todo List:**
    - [ ] Setup an integration test project using `xUnit` and `FluentAssertions`.
    - [ ] Write an integration test case for a complete successful happy path saga execution.
    - [ ] Write an integration test case where the `Compliance` step artificially fails, verifying that the `AccountBalance` state is fully compensated and rolled back.
*   **Definition of Done:**
    - Test pipeline passes locally.
    - Compensation logic is fully asserted without relying on actual external Docker containers (using local test harness).

### 📌 Issue #5: Introduce Distributed Locks and Rules Engine
*   **Git Branch:** `feature/issue-5`
*   **Description:** Protect the Balance service from concurrency issues and race conditions under heavy load using Redis distributed locks, and migrate compliance validations into an expandable declarative JSON structure.
*   **Todo List:**
    - [ ] Add a `Redis` instance into the `docker-compose.yml` file.
    - [ ] Integrate `RedLock.net` inside `HoldMoneyActivity` to lock account IDs mid-transaction.
    - [ ] Install `Microsoft.RulesEngine` NuGet package in `Compliance.Service` and load threshold definitions from a local JSON config.
*   **Definition of Done:**
    - Concurrent requests to the same account ID are queued/handled safely via Redis without balance race conditions.
    - Compliance service dynamically evaluates transactions based on externalized JSON rules.

---

## 📊 Stage 3: Real-Time Analytical Contour (DWH) & Data Masking

### 📌 Issue #6: Setup Real-Time Analytics Pipeline with Debezium and ClickHouse
*   **Git Branch:** `feature/issue-6`
*   **Description:** Build an isolated real-time analytical layer. Capture changes from multiple isolated Postgres databases via WAL logs using Change Data Capture (CDC) without affecting production transactional performance.
*   **Todo List:**
    - [ ] Add `Debezium (Kafka Connect)`, `ClickHouse`, and `Metabase` containers to `docker-compose.yml`.
    - [ ] Register Postgres source connectors in Debezium for all three microservice databases.
    - [ ] Create raw Staging tables in ClickHouse linked to Kafka engine topics.
    - [ ] Implement ClickHouse `Materialized Views` to transform, join, and aggregate data streams into a flat analytic cube.
*   **Definition of Done:**
    - Inserting data into transactional Postgres databases automatically streams data to ClickHouse in real-time with zero manual SQL selects.
    - Metabase successfully connects to ClickHouse pre-aggregated data marts to render financial reports.

### 📌 Issue #7: Prod-to-Test Data Masking Pipeline
*   **Git Branch:** `feature/issue-7`
*   **Description:** Create a secure pipeline that replicates production database transaction streams into a dedicated Test DB while anonymizing sensitive PII data deterministically using salted hashes.
*   **Todo List:**
    - [ ] Add a target `PostgreSQL-Test` instance to the infrastructure topology.
    - [ ] Create a pipeline consumer/transform script that intercepts incoming production CDC streams.
    - [ ] Apply deterministic salted hashing to client IDs and mask names/amounts before writing to the Test DB.
*   **Definition of Done:**
    - Test database populates automatically with real-looking production-like structures, but contains entirely obfuscated and safe anonymized records.

---

## 🌐 Stage 4: Cloud-Native Migration (Kubernetes Deployment)

### 📌 Issue #8: Kubernetes and Helm Migration
*   **Git Branch:** `feature/issue-8`
*   **Description:** Transition away from Docker Compose and prepare the entire platform topology for running inside a scalable, cloud-native orchestration environment.
*   **Todo List:**
    - [ ] Write optimized, multi-stage `Dockerfile`s for all .NET microservices.
    - [ ] Initialize a structured Helm Chart hierarchy inside `/deploy/k8s/charts`.
    - [ ] Define Kubernetes Deployments, Cluster Services, ConfigMaps, and CPU/Memory resource limits for every component.
*   **Definition of Done:**
    - Running `helm install` fully provisions the entire cluster environment (App services + Kafka + Redis + Postgres) inside a local K3s or Kind cluster.

---

## 🌋 Stage 5: High-Load Simulation & Chaos Engineering

### 📌 Issue #9: Chaos Engineering and Load Testing with k6
*   **Git Branch:** `test/issue-9`
*   **Description:** Execute the ultimate architectural validation. Bombard the Kubernetes cluster with heavy load and simulate infrastructure crash scenarios to prove system-wide data consistency.
*   **Todo List:**
    - [ ] Write a javascript load testing script using `k6` to simulate thousands of continuous ledger transactions.
    - [ ] Trigger an abrupt pod eviction (`kubectl delete pod`) of a processing node mid-flight during the test.
    - [ ] Inject network delays between applications and Redis to simulate split-brain conditions.
*   **Definition of Done:**
    - The system achieves Exactly-Once processing guarantees.
    - Zero financial records are dropped or corrupted, and MassTransit successfully tracks or compensates interrupted routing slips.
