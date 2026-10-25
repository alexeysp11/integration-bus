# Running and Testing the Platform

This guide takes you from a clean checkout to a fully verified stack: automated tests, the Docker Compose stack, the
identity and edge layers, horizontal scaling, the chaos and load scenarios and the Kubernetes deployment. Every section can be run independently.

---

## 1. Prerequisites

| Tool | Version | Used for |
| :--- | :--- | :--- |
| Docker (Desktop or Engine) with Compose v2 | recent | the whole stack, Testcontainers, k6 |
| .NET SDK | 10.0 | build and automated tests |
| `bash`, `curl`, `awk` | any | helper scripts (on Windows use Git Bash or WSL) |
| `kind`, `kubectl`, `helm` | kind 0.25+, helm 3.16+ | only for the Kubernetes sections |

Docker needs about 8 GB of free memory for the full stack. All commands below run from the repository root.

---

## 2. Automated Tests

```bash
dotnet test IntegrationBus.slnx
```

Several projects start real PostgreSQL, Redis, Kafka and ClickHouse containers through Testcontainers, so Docker must
be running; the first run pulls the images. Tests of this stage:

| Project | What it proves |
| :--- | :--- |
| `IntegrationBus.Shared.Tests` | Kafka partition keys: every record carries the account id as key, one account maps to one partition, and two consumers in one group process each account on one replica in production order. |
| `IntegrationBus.CoreLedger.Service.Tests` | Idempotent ledger insert, real compensation, routing slip completion tracking. |
| `IntegrationBus.AccountBalance.Service.Tests` | Concurrent writers on one account get unique sequence numbers, and a redelivered transaction is posted once. |
| `IntegrationBus.Gateway.Api.Tests`, `IntegrationBus.Identity.Service.Tests` | Token enforcement (401/503), token validation with locally generated signing keys. |

---

## 3. Docker Compose Stack

Start everything (the first build takes several minutes):

```bash
docker compose up -d --build
```

To run the Redis fault-injection scenarios later, start the stack through the Toxiproxy hop instead (it adds a proxy
between the services and Redis and is transparent until a fault is injected):

```bash
REDIS_ENDPOINT=integration-bus-toxiproxy:6380 docker compose --profile chaos up -d --build
```

Wait until `docker compose ps` shows the one-shot containers as `Exited (0)` and the rest as `Up`. Keycloak needs about
a minute to become healthy.

| Endpoint | URL |
| :--- | :--- |
| NGINX edge (HTTPS, self-signed certificate) | `https://localhost:8443` |
| Gateway (direct, still requires a token) | `http://localhost:5038` |
| Processing.Api (no authentication, used by the load tests) | `http://localhost:5201` |
| Keycloak admin console (`admin` / `admin`) | `http://localhost:8180` |
| Identity.Service health | `http://localhost:6006/health` |
| Worker replicas, health | host ports from `6010-6049` (`docker compose ps` lists them) |
| Kafka UI / Grafana (`admin`/`admin`) / Jaeger / Prometheus | `:8080` / `:3000` / `:16686` / `:9090` |
| Toxiproxy API (chaos profile only) | `http://localhost:8474` |

Check the replicas and that Prometheus scrapes each of them (`http://localhost:9090/targets` must show every replica as
`UP`):

```bash
docker compose ps --format '{{.Name}}\t{{.Status}}' | sort
```

For the full end-to-end business scenario (accounts, transaction, saga, observability, analytics) follow
[`business-logic/validation-guide.md`](business-logic/validation-guide.md).

---

## 4. Edge, Identity and Tokens

Obtain a token from Keycloak:

```bash
TOKEN=$(curl -s -X POST http://localhost:8180/realms/integration-bus/protocol/openid-connect/token \
  -d grant_type=client_credentials -d client_id=integration-bus-client \
  -d client_secret=integration-bus-client-secret | sed -E 's/.*"access_token":"([^"]+)".*/\1/')
echo "${TOKEN:0:20}..."
```

| Check | Command | Expected |
| :--- | :--- | :--- |
| No token is rejected | `curl -sk -o /dev/null -w "%{http_code}\n" -X POST https://localhost:8443/api/v1/ledger/transaction -H "Content-Type: application/json" -d '{}'` | `401` |
| Garbage token is rejected | same command with `-H "Authorization: Bearer garbage"` | `401` |
| Valid token reaches the API | same command with `-H "Authorization: Bearer $TOKEN"` | `400` (empty body fails validation, which proves the request reached `Processing.Api`) |
| Plain HTTP is refused | `curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8088/api/v1/ledger/transaction` | `426` |
| Rate limit | the loop below | a mix of `202` and `429` |
| Fail closed | `docker compose stop integration-bus-identity-service`, repeat the valid-token call | `503`; start the service again afterwards |

```bash
for i in $(seq 1 80); do
  curl -sk -o /dev/null -w "%{http_code}\n" -X POST \
    https://localhost:8443/api/v1/accounts/00000000-0000-0000-0000-000000000001/topup \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{"amount":1,"currency":1}'
done | sort | uniq -c
```

The edge log shows the limited requests (`limit=REJECTED`): `docker compose logs integration-bus-nginx | grep REJECTED | head`.
Design, configuration keys and troubleshooting: [`api-gateway/README.md`](api-gateway/README.md).

---

## 5. Scaling and Partition Keys

Consumer groups must show several members, each owning a share of the 12 partitions:

```bash
docker exec integration-bus-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9094 --describe --group balance-service-group
```

After sending a few top-ups through Processing.Api, the records of one account share one partition and carry the account
id as key:

```bash
docker exec integration-bus-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9094 --topic accounting-balance-topup --from-beginning --timeout-ms 5000 \
  --property print.key=true --property print.partition=true
```

Details: [`scaling/kafka-partition-keys.md`](scaling/kafka-partition-keys.md).

---

## 6. Load and Chaos Scenarios

Scenarios run through [`tests/chaos/scripts/run-chaos.sh`](../tests/chaos/scripts/run-chaos.sh): it prepares a pool of
funded accounts, runs k6 from the `grafana/k6` image, injects the fault and then runs the reconciliation. The Redis
scenarios need the stack started with `REDIS_ENDPOINT` as shown in section 3.

```bash
tests/chaos/scripts/run-chaos.sh baseline 2m
tests/chaos/scripts/run-chaos.sh kill-balance 3m
tests/chaos/scripts/run-chaos.sh kill-ledger 3m
tests/chaos/scripts/run-chaos.sh kill-orchestrator 3m
tests/chaos/scripts/run-chaos.sh redis-latency 2m
tests/chaos/scripts/run-chaos.sh redis-blackhole 2m
```

`TARGET_RPS` (default `200`) and `ACCOUNT_COUNT` (default `300`) can be overridden. Interpreting the result:

* The k6 summary at the end reports the number of acknowledged transactions and the latency percentiles. A threshold
  failure of the API itself (the `http_req_failed` or `http_req_duration` checks) is a finding.
* The reconciliation prints one `PASS`/`FAIL` line per invariant and the script exits non-zero if any invariant fails.
  Expected outcome: every line is `PASS`. Business rejections (`Failed` sagas) are normal; a `FAIL` line means a
  transaction was dropped, duplicated, left a hold or left an orphan record.
* Artifacts of every run are in `tests/chaos/work/<scenario>/` (account pool, k6 summary, per-database exports).

Invariants, scenario catalogue and the runbook: [`chaos-engineering/README.md`](chaos-engineering/README.md). If a scenario
fails, save the full console output together with `docker compose logs --since 15m` of the affected service.

---

## 7. Kubernetes (Kind)

```bash
kind create cluster --config deploy/k8s/kind/kind-cluster.yaml
docker compose build
for img in gateway-api processing-api saga-orchestrator-service account-balance-service compliance-service \
           core-ledger-service identity-service kafka-connect; do
  kind load docker-image "integration-bus-integration-bus-$img:latest" --name integration-bus
done
helm install integration-bus deploy/k8s/charts/integration-bus -n integration-bus --create-namespace \
  --set chaos.toxiproxy.enabled=true --timeout 10m
kubectl -n integration-bus get pods
```

Kind publishes every port with an offset of 10000: edge `https://localhost:18443`, Keycloak `http://localhost:18180`,
Processing.Api `http://localhost:15201`, Prometheus `http://localhost:19090`, Toxiproxy `http://localhost:18474`. Obtain
the token from `http://localhost:18180` and use `https://localhost:18443` for the section 4 checks.

Run the chaos scenarios against the cluster (the pods are killed with `kubectl delete pod --grace-period=0 --force`):

```bash
BACKEND=k8s tests/chaos/scripts/run-chaos.sh kill-ledger 3m
BACKEND=k8s tests/chaos/scripts/run-chaos.sh redis-latency 2m
```

Notes: some pods restart once while Kafka and Postgres become ready; wait until `kubectl get pods` shows all pods
`Running`. Details of the chart: [`k8s-deployment/README.md`](k8s-deployment/README.md).

---

## 8. Cleanup

```bash
docker compose --profile chaos down -v      # Docker Compose stack including volumes
kind delete cluster --name integration-bus  # Kubernetes cluster
```

---

## 9. Troubleshooting

| Symptom | Fix |
| :--- | :--- |
| A port is already in use | Stop the other process or the previous stack; Kind uses the Compose ports plus 10000, so both can run side by side. |
| `401` for a freshly issued token | Request the token through `http://localhost:8180` (Compose) or `http://localhost:18180` (Kind): the issuer must match `KC_HOSTNAME`. |
| `503` from the edge or gateway | The identity service is down or Keycloak is not healthy yet: `docker compose ps`, `curl http://localhost:6006/health`. |
| Redis scenario: "Toxiproxy is not reachable" | The stack was not started with `REDIS_ENDPOINT=integration-bus-toxiproxy:6380` and `--profile chaos` (Compose) or `chaos.toxiproxy.enabled=true` (Helm). |
| `docker run` for k6 cannot reach the API | On Linux the script maps `host.docker.internal` to the host gateway; make sure port `5201` (Kind: `15201`) is reachable from the host. |
| Tests fail pulling images | Check Docker connectivity and run `docker pull` for the failing image manually. |
