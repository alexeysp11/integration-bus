# Context for AI Assistant: integration-bus

## 🎯 Core Goal
The objective is to build a highly resilient, enterprise-grade distributed financial system using a **Database-per-Service** architecture. 
**Crucial Mindset:** No custom frameworks or "reinventing the wheel". We act as system integrators, connecting industry-standard, production-ready tools (MassTransit, Kafka, ClickHouse, Debezium) to solve infrastructure challenges. Business logic must remain minimal (if/else mocks).

## 🛠️ Architecture Blueprint
1. **API Gateway (YARP)** receives a financial transaction request.
2. **Web API Ledger Gateway** kicks off an asynchronous Distributed Saga using **MassTransit Courier (Routing Slips)** via **Apache Kafka**.
3. **Saga Steps (Isolated Services & DBs):**
   - `Account Balance Service` (Deducts balance, uses Redis Distributed Locks via RedLock.net).
   - `Compliance Service` (Evaluates risks using local JSON config via RulesEngine).
   - `Core Ledger Service` (Writes immutable transaction audit trail to PostgreSQL).
4. **Analytics Contour (DWH):** **Debezium CDC** captures Postgres WAL logs -> streams to **Kafka** -> feeds into **ClickHouse OLAP** -> builds aggregated ROLAP Cubes via **Materialized Views** -> visualizes via **Metabase**.
5. **Data Masking:** Anonymizes production data streams using deterministic salted hashes to populate a safe `PostgreSQL Test DB`.

## 🌋 Current Focus
We are at **Stage 1**: Setting up the project structure, configuring MassTransit with Apache Kafka inside .NET 9 Web API (`GatewayApi`), and drafting the initial `docker-compose.yml`.
