# 🗺️ Project Roadmap & Backlog: integration-bus

This document outlines the complete iterative implementation plan for the `integration-bus` platform, broken down into 5 sequential stages and 9 actionable issues.

---

## 🚀 Stage 1: Core .NET Architecture & Asynchronous Saga (MVP)

### 📌 Issue #1: Base Infrastructure and Gateway Skeleton
*   **Git Branch:** `feature/issue-1-infra-skeleton`
*   **Description:** Establish the foundational infrastructure and project layout for the `integration-bus` platform. The goal is to spin up the core message broker and database containers, initialize the initial .NET 9 Web API gateway project, and verify its automated integration with Apache Kafka via MassTransit on startup.
*   **Todo List:**
    - [ ] Create the root repository directory structure (`src/`, `deploy/`).
    - [ ] Initialize the `GatewayApi` project using .NET 9 Web API in the `src/GatewayApi/` folder.
    - [ ] Install required NuGet packages (`MassTransit`, `MassTransit.Kafka`).
    - [ ] Draft a minimal `deploy/docker-compose.yml` containing Apache Kafka (KRaft mode) and a single PostgreSQL instance.
    - [ ] Implement the baseline bus initialization in `Program.cs` for `GatewayApi`, configuring the transport to connect to the local Kafka broker.
*   **Definition of Done:**
    - The project compiles successfully without errors or warnings.
    - Running `docker-compose up -d` spins up the infrastructure layer smoothly.
    - `GatewayApi` boots up and successfully connects to the Kafka broker, automatically declaring system topologies/topics without crashing.

### 📌 Issue #2: Project Skeletons for Saga Participants
*   **Git Branch:** `feature/issue-2-service-skeletons`
*   **Description:** Following the **Database-per-Service** architectural pattern, we need to isolate the execution contexts for each distributed transaction step. This task involves creating three independent .NET 9 worker services and provisioning dedicated, isolated database containers for each context inside the Docker environment.
*   **Todo List:**
    - [ ] Initialize the `AccountBalance.Service` project (.NET 9 Worker / Web API).
    - [ ] Initialize the `Compliance.Service` project (.NET 9 Worker / Web API).
    - [ ] Initialize the `CoreLedger.Service` project (.NET 9 Worker / Web API).
    - [ ] Expand `deploy/docker-compose.yml` by adding three isolated PostgreSQL databases (`balance_db`, `compliance_db`, `ledger_db`).
    - [ ] Configure baseline MassTransit and Kafka consumer connections across all three new services.
*   **Definition of Done:**
    - Dedicated project directories and structures are established for all three Saga participants.
    - Each service runs independently and has access exclusively to its own isolated database.
    - The updated docker-compose environment spins up segregated storages, preventing direct database-level coupling between contexts.

### 📌 Issue #3: Asynchronous Saga Orchestration via MassTransit Courier
*   **Git Branch:** `feature/issue-3-saga-orchestration`
*   **Description:** Implement the end-to-end asynchronous distributed transaction flow using the Saga Orchestration pattern. The `GatewayApi` must intercept incoming HTTP requests, construct an immutable Routing Slip, and dispatch it to the bus. Each microservice must execute its corresponding activity step, commit states locally, and provide compensation logic for automated rollbacks.
*   **Todo List:**
    - [ ] Implement `HoldMoneyActivity` and its corresponding compensation (funds reversal) inside `AccountBalance.Service`.
    - [ ] Implement `ComplianceCheckActivity` (limits validation) inside `Compliance.Service`.
    - [ ] Implement `CommitLedgerActivity` (immutable transaction log entry) inside `CoreLedger.Service`.
    - [ ] Configure the controller in `GatewayApi` to accept the financial payload and assemble the `RoutingSlip` sequence.
    - [ ] Verify the end-to-end happy path execution flow via MassTransit telemetry logs.
*   **Definition of Done:**
    - Incoming HTTP POST requests to the gateway successfully trigger the asynchronous Saga execution.
    - Messages flow sequentially across microservices via dedicated Kafka topics.
    - Upon successful execution, states are consistently updated across all three isolated databases (funds held -> compliance approved -> ledger committed).

---

## 🧪 Stage 2: Reliability Engineering & Integration Testing

### 📌 Issue #4: Integration Testing for Saga Compensations
*   **Git Branch:** `test/issue-4-saga-integration-tests`
*   **Description:** Add a comprehensive integration test suite using WebApplicationFactory and the MassTransit test harness to ensure that infrastructure and business validation errors trigger the correct automated rollback behaviors.
*   **Todo List:**
    - [ ] Setup an integration test project using `xUnit` and `FluentAssertions`.
    - [ ] Write an integration test case for a complete successful happy path saga execution.
    - [ ] Write an integration test case where the `Compliance` step artificially fails, verifying that the `AccountBalance` state is fully compensated and rolled back.
*   **Definition of Done:**
    - Test pipeline passes locally.
    - Compensation logic is fully asserted without relying on actual external Docker containers (using local test harness).

### 📌 Issue #5: Introduce Distributed Locks and Rules Engine
*   **Git Branch:** `feature/issue-5-resilience-logic`
*   **Description:** Protect the Balance service from concurrency issues and race conditions under heavy load using Redis distributed locks, and migrate compliance validations into an expandable declarative JSON structure.
*   **Todo List:**
    - [ ] Add a `Redis` instance into the `deploy/docker-compose.yml` file.
    - [ ] Integrate `RedLock.net` inside `HoldMoneyActivity` to lock account IDs mid-transaction.
    - [ ] Install `Microsoft.RulesEngine` NuGet package in `Compliance.Service` and load threshold definitions from a local JSON config.
*   **Definition of Done:**
    - Concurrent requests to the same account ID are queued/handled safely via Redis without balance race conditions.
    - Compliance service dynamically evaluates transactions based on externalized JSON rules.

---

## 📊 Stage 3: Real-Time Analytical Contour (DWH) & Data Masking

### 📌 Issue #6: Setup Real-Time Analytics Pipeline with Debezium and ClickHouse
*   **Git Branch:** `feature/issue-6-cdc-dwh-analytics`
*   **Description:** Build an isolated real-time analytical layer. Capture changes from multiple isolated Postgres databases via WAL logs using Change Data Capture (CDC) without affecting production transactional performance.
*   **Todo List:**
    - [ ] Add `Debezium (Kafka Connect)`, `ClickHouse`, and `Metabase` containers to `deploy/docker-compose.yml`.
    - [ ] Register Postgres source connectors in Debezium for all three microservice databases.
    - [ ] Create raw Staging tables in ClickHouse linked to Kafka engine topics.
    - [ ] Implement ClickHouse `Materialized Views` to transform, join, and aggregate data streams into a flat analytic cube.
*   **Definition of Done:**
    - Inserting data into transactional Postgres databases automatically streams data to ClickHouse in real-time with zero manual SQL selects.
    - Metabase successfully connects to ClickHouse pre-aggregated data marts to render financial reports.

### 📌 Issue #7: Prod-to-Test Data Masking Pipeline
*   **Git Branch:** `feature/issue-7-data-anonymization`
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
*   **Git Branch:** `feature/issue-8-kubernetes-migration`
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
*   **Git Branch:** `chaos/issue-9-load-and-resiliency-testing`
*   **Description:** Execute the ultimate architectural validation. Bombard the Kubernetes cluster with heavy load and simulate infrastructure crash scenarios to prove system-wide data consistency.
*   **Todo List:**
    - [ ] Write a javascript load testing script using `k6` to simulate thousands of continuous ledger transactions.
    - [ ] Trigger an abrupt pod eviction (`kubectl delete pod`) of a processing node mid-flight during the test.
    - [ ] Inject network delays between applications and Redis to simulate split-brain conditions.
*   **Definition of Done:**
    - The system achieves Exactly-Once processing guarantees.
    - Zero financial records are dropped or corrupted, and MassTransit successfully tracks or compensates interrupted routing slips.
