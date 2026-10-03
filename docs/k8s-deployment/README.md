# ☸️ Kubernetes Deployment: `integration-bus` via Helm (Kind/K3s)

[English](README.md) | [Русский](README.ru.md)

This document describes the translation of the full `docker-compose.yml` stack (18 workloads: 6 .NET services,
Kafka, Postgres, Redis, Kafka Connect/Debezium, ClickHouse, Metabase, Prometheus, Grafana, Loki, Jaeger, Kafka UI,
Redis Commander) into a single Helm chart, deployable with one `helm install` command into a local Kubernetes
cluster (verified on **Kind**). This closes Stage 5 of [`docs/roadmap.md`](../roadmap.md), part of the project's
broader backlog.

> 🧑‍🎓 **Never worked with Kubernetes before?** Start with [`GETTING-STARTED.md`](GETTING-STARTED.md) — a
> step-by-step "what to click and download" walkthrough for complete beginners. This document (below) is a
> technical breakdown of the architecture and the issues found, for those who have already deployed the stack and
> want to understand how it is built.

---

## 1. Artifact Layout

```text
deploy/k8s/
├── kind/
│   └── kind-cluster.yaml          # local Kind cluster configuration with port mappings
└── charts/
    └── integration-bus/
        ├── Chart.yaml
        ├── values.yaml              # all images, passwords, nodePorts in one place
        ├── files/                   # copies of infrastructure/*.sql, *.json, *.yml, *.sh (see §5)
        └── templates/
            ├── secrets.yaml
            ├── configmaps.yaml
            ├── postgres.yaml        # StatefulSet + PVC
            ├── redis.yaml           # StatefulSet + PVC, + Redis Commander
            ├── kafka.yaml           # StatefulSet + PVC, headless Service, kafka-provisioner Job, Kafka UI
            ├── kafka-connect.yaml   # Deployment + debezium-registrar Job
            ├── clickhouse.yaml      # StatefulSet + PVC
            ├── metabase.yaml        # Deployment + metabase-driver-setup Job + PVC
            ├── prometheus.yaml
            ├── grafana.yaml
            ├── jaeger.yaml
            ├── loki.yaml            # StatefulSet + PVC
            └── app-services.yaml    # all 6 .NET services from one DRY template (range over values.services)
```

The services' Dockerfiles were **not changed** — all six already had correct multi-stage builds, suitable for any
container runtime. The only infrastructure work was describing the topology in Kubernetes manifests.

---

## 2. Quick Start

```bash
# 1. Bring up the local cluster (once)
kind create cluster --config deploy/k8s/kind/kind-cluster.yaml

# 2. Build the service images (same as for docker-compose) and load the custom ones into the cluster
docker compose build
for img in integration-bus-integration-bus-gateway-api \
           integration-bus-integration-bus-processing-api \
           integration-bus-integration-bus-saga-orchestrator-service \
           integration-bus-integration-bus-account-balance-service \
           integration-bus-integration-bus-compliance-service \
           integration-bus-integration-bus-core-ledger-service \
           integration-bus-integration-bus-kafka-connect; do
  kind load docker-image "$img:latest" --name integration-bus
done

# 3. Deploy the entire stack with a single command
kubectl create namespace integration-bus
helm install integration-bus deploy/k8s/charts/integration-bus -n integration-bus --timeout 10m
```

`helm install` itself:
1. Brings up Postgres/Redis/Kafka/ClickHouse (as `StatefulSet`s with a `PersistentVolumeClaim`) and all 6 .NET
   services.
2. Runs post-install `Job` hooks in sequence: `kafka-provisioner` (creates all Kafka topics) →
   `metabase-driver-setup` (downloads the ClickHouse driver) → `debezium-registrar` (registers the 3 Debezium
   source + 3 ClickHouse sink connectors).

Public images (`postgres`, `redis`, `apache/kafka`, etc.) do **not** need to be loaded via `kind load` — the Kind
node has internet access and pulls them directly from the registry as soon as the matching pods are scheduled.
`kind load docker-image` **does not work** for them in this environment, due to a known incompatibility with
Docker Desktop's containerd-backed image store (see §4.4) — for our 7 self-built images (the only ones with no
registry), this approach works correctly.

### Accessing the Services

If the cluster was created via `deploy/k8s/kind/kind-cluster.yaml`, ports are mapped onto `localhost` using the
convention **host port = docker-compose port + 10000** (so Kind can run alongside an already-running
docker-compose stack on the same machine with no conflicts):

| Service | docker-compose | Kind (localhost) |
|---|---|---|
| Gateway.Api | 5038 | **15038** |
| Processing.Api | 5201 | **15201** |
| SagaOrchestrator /health | 6001 | **16001** |
| AccountBalance /health | 6002 | **16002** |
| Compliance /health | 6003 | **16003** |
| CoreLedger /health | 6004 | **16004** |
| Grafana | 3000 | **13000** |
| Jaeger UI | 16686 | **26686** |
| Prometheus | 9090 | **19090** |
| Kafka UI | 8080 | **18080** |
| Redis Commander | 8082 | **18082** |
| Metabase | 3001 | **13001** |
| Kafka Connect REST | 8083 | **18083** |

---

## 3. Translating the docker-compose Topology to Kubernetes

The key technique that simplified the port: **Kubernetes `Service` names match docker-compose container names
1:1** (`integration-bus-kafka`, `integration-bus-db`, `integration-bus-redis`, `integration-bus-loki`,
`integration-bus-jaeger`, etc.). Thanks to this, **not a single service environment variable had to change** —
`Kafka__BootstrapServers=integration-bus-kafka:9094`, `ConnectionStrings__SagaDb=Host=integration-bus-db;...`, and
the entire Serilog/OTel block moved from `docker-compose.yml` into `values.yaml` completely unmodified.

Other typical transformations:
- Compose's one-shot "init" containers (`kafka-provisioner`, `debezium-registrar`, `metabase-driver-setup`)
  became Helm post-install `Job` hooks (`helm.sh/hook: post-install,post-upgrade`), with a `hook-weight` for the
  correct order: topics first, then connector registration.
- `depends_on: condition: service_healthy` was replaced with `initContainers` doing a simple
  `nc -z <service> <port>` — waiting for the TCP port to become reachable before starting the main container.
- Stateful services (Postgres, Redis, Kafka, ClickHouse, Loki) became `StatefulSet`s with
  `volumeClaimTemplates` — Kind ships a default `StorageClass` (`rancher.io/local-path`) out of the box, and PVCs
  bind automatically.
- The 6 identically-shaped .NET services are not manually duplicated 6 times; they are described by a single
  `app-services.yaml` template with `range $name, $svc := .Values.services` over a map in `values.yaml` — adding a
  seventh service means adding one block to `values.yaml`, not copying 70 lines of YAML.

---

## 4. Troubleshooting: Common Symptoms When Deploying to Kubernetes

Below is a diagnostic reference for non-trivial failure scenarios that are specific to running this stack on
Kubernetes (as opposed to docker-compose). The chart's configuration already accounts for every fix listed here;
this section is useful if you modify the chart's templates or deploy a similar topology yourself.

### 4.1. Kafka's Self-Connect Through a Plain ClusterIP — a Deadlock

**Symptom:** the `integration-bus-kafka-0` pod restarted endlessly with
`Connection to node 1 (integration-bus-kafka/10.96.x.x:9093) could not be established`.

**Cause:** `KAFKA_CONTROLLER_QUORUM_VOTERS=1@integration-bus-kafka:9093` makes the single broker connect **to
itself** via the Service DNS name `integration-bus-kafka` to establish the KRaft quorum. But a plain `ClusterIP`
Service only routes traffic to pods in the **Ready** state — and the pod cannot become Ready until it connects to
itself. A closed loop. This never surfaced in docker-compose, because Docker's built-in DNS resolves a container
name directly to its IP, with no dependency on health-check status.

**Fix:** the Kafka Service is made headless (`clusterIP: None`) with `publishNotReadyAddresses: true` — then DNS
returns the pod's IP regardless of its readiness.

### 4.2. Liveness/Readiness Probe Timeout Shorter Than the JVM Utility's Startup Time

**Symptom:** Kafka entered `CrashLoopBackOff` with `Liveness probe failed: command timed out ... after 1s`.

**Cause:** the `exec` probe invokes `kafka-broker-api-versions.sh` — a wrapper around the JVM, which needs more
than 1 second to start up. Kubernetes's default `timeoutSeconds` for `exec` probes is **1 second**, and without an
explicit override it is not enough for this probe.

**Fix:** `timeoutSeconds: 10` on both the readiness and liveness probes.

### 4.3. Missing PersistentVolumeClaim for Kafka — Topics Lost on Restart

**Symptom:** after a manual `kubectl delete pod integration-bus-kafka-0` (to apply the §4.2 fix), every topic
created by the `kafka-provisioner` Job disappeared, and all 6 .NET services crashed with
`Confluent.Kafka.ConsumeException: Subscribed topic not available`.

**Cause:** `/tmp/kafka-logs` (this image's actual `log.dirs`) was not mounted on any `PersistentVolumeClaim` — it
is an ordinary container filesystem, which is wiped every time the pod is recreated.

**Fix:** added a `volumeClaimTemplates` entry for Kafka (as Postgres/Redis/ClickHouse/Loki already had).

> `volumeClaimTemplates` is immutable in Kubernetes once a `StatefulSet` is created — it cannot be added to an
> existing StatefulSet, only by recreating the resource. For a demo/dev cluster, the simplest approach is to
> delete and recreate the namespace (`kubectl delete namespace integration-bus`), which is what was done for the
> final clean verification.

### 4.4. `kind load docker-image` Does Not Work for Public Images

**Symptom:** `ERROR: failed to load image: ... ctr: content digest sha256:... not found` for every image except
our 7 self-built ones.

**Cause:** a known incompatibility between `kind load docker-image` and the containerd-backed image store that
Docker Desktop uses by default in newer versions — `docker save` does not serialize every content blob referenced
by a multi-platform manifest.

**Workaround:** public images do not need `kind load` at all — pods just need `imagePullPolicy: IfNotPresent` (or
the default behavior), and containerd on the Kind node simply pulls the image from the internet directly on first
pod placement. This only matters for our 7 custom images, which have no external registry.

### 4.5. Control-Plane Instability When Running Alongside docker-compose

**Symptom:** `kube-scheduler` and `kube-controller-manager` entered `CrashLoopBackOff` with
`context deadline exceeded` when contacting `etcd`/`kube-apiserver`; as a result, the `Endpoints` controller
failed to mark `clickhouse` as live in time, despite it passing readiness.

**Cause:** running the full docker-compose stack (18 containers) **and** the full Kind cluster (another 18
workloads + control plane) simultaneously on one machine overloaded disk/CPU enough that control-plane components
could not renew their leader-election lease in time.

**Workaround:** `docker compose down` (without `-v`, volumes preserved) before the final verification of the K8s
stack. The two stacks are **not meant to run simultaneously** on one machine with limited resources — this is an
expected architectural constraint of local development, not a chart bug.

### 4.6. Self-Healing Race Condition on First Startup

**Observation (not a bug, documented as-is):** on first deployment, 1–2 of the 6 .NET services may fail with
`Subscribed topic not available` and are automatically restarted by Kubernetes (`restartPolicy: Always`), because
their own pod starts slightly before the `kafka-provisioner` Job finishes creating all the topics. This is safe:
after 1–2 restarts (usually under a minute), every pod stably transitions to `1/1 Running` with no manual
intervention, since the topics already exist by the time of the next connection attempt.

---

## 5. Infrastructure Files

The chart's ConfigMaps embed the contents of `infrastructure/postgres/init.sql`,
`infrastructure/clickhouse/init.sql`, `infrastructure/prometheus/prometheus.yml`,
`infrastructure/debezium/*.json` + `register-connectors.sh`, and `infrastructure/clickhouse-sink/*.json` via
`.Files.Get` over copies of them in `deploy/k8s/charts/integration-bus/files/`. Helm cannot read files outside
the chart directory, so these copies are unavoidable — **when changing the originals in `infrastructure/`, do not
forget to sync the copies in `deploy/k8s/charts/integration-bus/files/`.**

---

## 6. Verified Live

A full "black box" run (as in [`docs/business-logic/validation-guide.md`](../business-logic/validation-guide.md)),
performed through the mapped Kind ports on a clean, freshly created namespace:
1. `POST /api/v1/accounts/seed` → `202`, accounts appeared in Postgres.
2. `POST /api/v1/accounts/{id}/topup` → `202`.
3. `POST /api/v1/ledger/transaction` → `202`, the saga reached `CurrentState = Completed`, `ErrorMessage = NULL`.
4. `SELECT * FROM analytics.transaction_cube WHERE TransactionId = '...'` in ClickHouse — the row is present,
   with every correct field (including having gone through the full Debezium → Kafka Connect → ClickHouse Sink
   cycle).
5. `GET /api/services` in Jaeger — all 6 .NET services are visible as trace sources.

---

## 7. Stopping and Cleaning Up

```bash
helm uninstall integration-bus -n integration-bus
kubectl delete namespace integration-bus   # also deletes all PVCs
kind delete cluster --name integration-bus # fully tears down the cluster
```
