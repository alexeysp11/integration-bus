# 📊 Observability Stack: Prometheus, Grafana, Loki, Jaeger

[English](README.md) | [Русский](README.ru.md)

A production-level, step-by-step guide to verifying and configuring the `integration-bus` observability stack.

---

## 1. What Was Added

| Component | Container | Port (host) | Purpose |
|---|---|---|---|
| Prometheus | `integration-bus-prometheus` | `9090` | Metrics collection (`/metrics`) from all 6 .NET services |
| Grafana | `integration-bus-grafana` | `3000` | Dashboards, Data Sources for Prometheus/Loki/Jaeger |
| Jaeger (all-in-one) | `integration-bus-jaeger` | `16686` (UI), `4317` (OTLP gRPC), `4318` (OTLP HTTP) | Distributed tracing |
| Loki | `integration-bus-loki` | `3100` | Centralized log storage |

The code (`IntegrationBus.Shared.Extensions.TelemetryExtensions`) adds an `AddDistributedTracing()` method that
registers:
- `AddSource("MassTransit")` — traces Kafka/MassTransit consume/produce operations;
- `AddAspNetCoreInstrumentation()` — inbound HTTP requests;
- `AddHttpClientInstrumentation()` — outbound HTTP calls (e.g. Gateway → Processing.Api);
- `AddEntityFrameworkCoreInstrumentation()` — Postgres SQL commands;
- `AddOtlpExporter()` — exports to Jaeger at the address from the `OTEL_EXPORTER_OTLP_ENDPOINT` environment
  variable (already set in `docker-compose.yml` as `http://integration-bus-jaeger:4317` for every service).

Each service's logs are written to three Serilog sinks (`Console`, `File`, `GrafanaLoki`), enriched with
`TraceId`/`SpanId` via `Serilog.Enrichers.Span` (`Enrich: ["FromLogContext", "WithSpan"]` in `appsettings.json`),
enabling a direct jump from a log line to its trace in Jaeger.

Every service also exposes `GET /health` (via `Microsoft.Extensions.Diagnostics.HealthChecks` +
`AspNetCore.HealthChecks.NpgSql/Kafka/Redis`), which checks the real connection to its dependencies (Postgres,
Kafka, and — for `account-balance-service`/`core-ledger-service` — Redis as well).

---

## 2. Verifying the Containers Started Successfully

```bash
docker compose up -d
docker compose ps
```

All services should be in the `Up` state (for `integration-bus-db`, `integration-bus-redis`,
`integration-bus-kafka` — `Up (healthy)`; `integration-bus-kafka-provisioner` — `Exited (0)`, which is expected,
since it is a one-shot job).

Checking the applications' health endpoints from the host:

```bash
curl http://localhost:5038/health   # Gateway.Api
curl http://localhost:5201/health   # Processing.Api
curl http://localhost:6001/health   # Saga Orchestrator
curl http://localhost:6002/health   # Account Balance
curl http://localhost:6003/health   # Compliance
curl http://localhost:6004/health   # Core Ledger
```

Each should return `Healthy` with `200 OK`. If one returns `Unhealthy`/`503`, it means the corresponding
dependency (Postgres/Kafka/Redis) is not ready yet — wait a few seconds and retry (Kafka provisioning and
database migrations run on first startup).

Checking Prometheus targets — open `http://localhost:9090/targets`: all 5 jobs (`processing-api`,
`saga-orchestrator`, `account-balance`, `compliance`, `core-ledger`) should show status `UP`.

Checking Jaeger — `http://localhost:16686` should open the UI with no errors.

Checking Loki — `curl http://localhost:3100/ready` should return `ready`.

---

## 3. Configuring Grafana: Connecting Data Sources

Open `http://localhost:3000` (login/password: `admin` / `admin` — Grafana will prompt a password change on first
login; you may skip it with the **Skip** button).

### 3.1 Prometheus
1. In the left menu, click the gear icon **Connections → Data sources**.
2. Click **Add data source**.
3. Select **Prometheus** from the list.
4. In the **URL** field, enter `http://integration-bus-prometheus:9090` (the container name is used, since
   Grafana reaches Prometheus inside the shared Docker network `integration-bus-network`, not via `localhost`).
5. Click **Save & Test** at the bottom of the page. A green message `Successfully queried the Prometheus API`
   should appear.

### 3.2 Loki
1. **Connections → Data sources → Add data source → Loki**.
2. In the **URL** field, enter `http://integration-bus-loki:3100`.
3. Click **Save & Test** — `Data source successfully connected` is expected.

### 3.3 Jaeger
1. **Connections → Data sources → Add data source → Jaeger**.
2. In the **URL** field, enter `http://integration-bus-jaeger:16686`.
3. Click **Save & Test**.

> After configuring Jaeger as a data source, in Grafana Explore on the Loki panel, logs enriched with `TraceId`
> automatically get a button to jump to the corresponding trace (a derived field), if you enable it in the Loki
> data source settings (**Loki → Settings → Derived fields → Add** → `Name: TraceId`,
> `Regex: "TraceId":"(\w+)"`, `Query: ${__value.raw}`, **Internal link → Jaeger**).

---

## 4. Importing Baseline Dashboards

### 4.1 Via Ready-Made Community Dashboards
1. In the left menu, open **Dashboards → New → Import**.
2. In the **Import via grafana.com** field, enter the ID of the dashboard you want to import (find a current one
   on [grafana.com/grafana/dashboards](https://grafana.com/grafana/dashboards/), filtering by the `Prometheus`
   datasource and keywords `ASP.NET Core` or `.NET`/`OpenTelemetry`).
3. On the next step, in the **Prometheus** dropdown, select the datasource created in §3.1, and click **Import**.

Ready-made dashboard IDs on grafana.com periodically change or become outdated, so do not rely on a hardcoded
number — the safer option is below.

### 4.2 Safer Option: Build a Minimal Dashboard Manually From Real Metrics
1. **Dashboards → New → New Dashboard → Add visualization** → select the **Prometheus** datasource.
2. In the query field, start typing `http_server` — Prometheus will show autocomplete suggestions for the actual
   metrics exported by `OpenTelemetry.Instrumentation.AspNetCore` (e.g. HTTP request duration and count per
   service).
3. Similarly, type `process_runtime_dotnet` — GC/heap/thread metrics from
   `OpenTelemetry.Instrumentation.Runtime` (memory, threads, garbage collections).
4. Type `masstransit` — MassTransit consumer/producer metrics (messages processed, processing duration, error
   count) from `AddMeter("MassTransit")`.
5. Save the dashboard (**Save dashboard**), giving it a name, e.g.
   `integration-bus: Runtime & MassTransit Overview`.

Since the metric names emitted by the `OpenTelemetry.Instrumentation.*` package versions pinned in this project
(see `src/Libraries/IntegrationBus.Shared/IntegrationBus.Shared.csproj`) may differ from what a randomly found
community dashboard displays, Prometheus's autocomplete in §4.2 is the most reliable way to see exactly the
metrics this solution actually exports.

---

## 5. End-to-End Verification: Log → Trace → Metric

1. Send a test transaction (see [`docs/business-logic/validation-guide.md`](../business-logic/validation-guide.md)
   or [`docs/business-logic/api-specifications.md`](../business-logic/api-specifications.md) for a ready-made
   `cURL`).
2. Copy the `transactionId` from the response.
3. In Grafana → **Explore** → select the **Loki** datasource → query
   `{service_name="integration-bus-saga-orchestrator-service"} |= "<transactionId>"`.
4. In the matching log line, find the `TraceId` (added by `Serilog.Enrichers.Span`) and open it in Jaeger
   (`http://localhost:16686/trace/<TraceId>`), or via the derived field configured in §3.3.
5. Jaeger should display a chain of spans across every service involved in the saga
   (`Processing.Api → SagaOrchestrator → AccountBalance → Compliance → CoreLedger`).

Result: a single `TraceId` matches across the logs of every involved service in Loki, and the corresponding trace
in Jaeger contains the complete chain across all 6 services
(`gateway-api → processing-api → saga-orchestrator-service → account-balance-service → compliance-service →
core-ledger-service`), including Kafka `send`/`receive`/`process` spans, the 3 Courier Routing Slip execute
activities (`WriteAuditTrail`, `UpdateCache`, `PublishLedgerCommitted`), and `saga_db` (Postgres) spans.

`SagaOrchestrator.Service` logs each of the saga's 4 steps at `Information` level (dispatching
`HoldAccountBalance`, `CheckComplianceLimits`, `WriteLedgerRecord`, `ConfirmAccountBalance`) and at `Warning`
level — every compensation and the technical fault path of `ProcessLedgerWriteActivity` — so the central
orchestrator is fully visible in Loki/Grafana alongside every other service.

### Troubleshooting: the Saga Runs but Is Invisible in Loki

If a new saga Activity is silent in the logs despite executing correctly, check that an `ILogger<T>` is injected
into its constructor and that `Execute`/`Faulted` contains an explicit call to
`logger.LogInformation`/`LogWarning`: MassTransit does not log State Machine steps on its own — that is the
responsibility of the specific `IStateMachineActivity`.
