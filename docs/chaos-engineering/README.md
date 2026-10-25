# Chaos Engineering, Load Testing & Horizontal Scaling

[English](README.md) | [Русский](README.ru.md)

The worker services run as several replicas, Kafka routes messages to replicas by partition key, and a repeatable
load and fault-injection harness checks that no financial record is lost, duplicated or corrupted while replicas are
killed and Redis is degraded.

---

## 1. Horizontal Scaling Topology

| Service | Replicas (Compose / Helm) |
| :--- | :--- |
| `AccountBalance.Service` | 3 |
| `SagaOrchestrator.Service`, `Compliance.Service`, `CoreLedger.Service` | 2 each |
| `Gateway.Api`, `Processing.Api` | 1 |

Every topic has 12 partitions (`PARTITIONS` in the Compose provisioner, `kafka.topicPartitions` in Helm), which divides
evenly across 2, 3, 4 or 6 replicas of one consumer group. All producers stamp records with a partition key
(`AccountId` or `TransactionId`), so one account or one transaction is always handled by one replica in production
order. Key selection and verification: [`docs/scaling/kafka-partition-keys.md`](../scaling/kafka-partition-keys.md).

Prometheus discovers replicas through DNS service discovery (`dns_sd_configs`): the Compose service name and the
Helm headless `*-pods` Service both resolve to one address per replica. Replica host ports in Compose are allocated
from the ranges `6010-6019` (orchestrator), `6020-6029` (balance), `6030-6039` (compliance) and `6040-6049` (ledger);
list them with `docker compose ps`.

---

## 2. Exactly-Once Processing Design

Kafka delivers at least once, so exactly-once *effects* come from idempotent consumers plus offsets that are committed
only after the work and its outcome event are done.

| Step | Duplicate / crash protection |
| :--- | :--- |
| Hold, Top-up, Confirm, Release (`AccountBalance.Service`) | Each consumer checks for an existing journal entry of the transaction and replays the outcome event instead of posting twice. A PostgreSQL transaction-scoped advisory lock on the `TransactionId` (and on every touched account, taken in ascending order) serializes duplicates and concurrent writers across all replicas, so two writers never read the same maximum sequence number. `HoldAccountBalanceConsumer` additionally holds a Redis RedLock per account. |
| Ledger write (`CoreLedger.Service`) | `LedgerEntries` has a unique index on `TransactionId` and the insert is `ON CONFLICT DO NOTHING`, so a redelivered command cannot create a second record. The compensation of `WriteAuditTrail` deletes the entry, so a transaction whose routing slip rolled back leaves no ledger record. |
| Routing slip (`CoreLedger.Service`) | `WriteLedgerRecordConsumer` waits (up to 30 s) until the in-memory Courier routing slip finished and its outcome event reached Kafka, tracked by `LedgerRoutingSlipCompletionRegistry`. The Kafka offset is therefore not committed while the slip is in flight: a replica killed mid-slip causes redelivery to a healthy replica, which re-runs the idempotent slip. A slip that outlives the timeout is retried three times at 2 s intervals. |
| Saga (`SagaOrchestrator.Service`) | Duplicate or late events (for example after a redelivery) are ignored in every state instead of faulting the consumer. The EF Core transactional outbox and consumer inbox protect the state transition itself. |

Redis is used only as an additional lock and cache. Journal correctness does not depend on it: even if Redis loses
or delays a lock, the PostgreSQL advisory locks and the unique constraints keep the journal consistent.

---

## 3. Tooling

| Path | Purpose |
| :--- | :--- |
| [`tests/chaos/k6/ledger-load.js`](../../tests/chaos/k6/ledger-load.js) | k6 script: constant-arrival-rate load, 85% debit sagas and 15% top-ups that keep the money supply in equilibrium (see [`load-testing-topup-strategy.md`](load-testing-topup-strategy.md)). Writes a JSON summary with the number of acknowledged transactions. |
| [`tests/chaos/scripts/prepare-accounts.sh`](../../tests/chaos/scripts/prepare-accounts.sh) | Seeds and funds a pool of accounts and writes their ids for k6. |
| [`tests/chaos/scripts/run-chaos.sh`](../../tests/chaos/scripts/run-chaos.sh) | Runs one scenario: k6 load, fault injection and reconciliation. |
| [`tests/chaos/scripts/reconcile.sh`](../../tests/chaos/scripts/reconcile.sh) | Waits for in-flight sagas to finish and verifies the invariants in section 5. |
| `infrastructure/toxiproxy/toxiproxy.json` | Toxiproxy proxy `redis` (`:6380` to Redis), used for network fault injection. |

The scripts need `bash`, `docker`, `curl` and `awk`; the Kubernetes backend also needs `kubectl`. k6 runs from the
`grafana/k6` image, so no local k6 installation is required. Load goes directly to `Processing.Api` (port `5201`,
Kind: `15201`) to measure the saga pipeline instead of the edge rate limits.

---

## 4. Scenarios

| ID | Scenario | Injected failure | Expected behavior |
| :--- | :--- | :--- | :--- |
| CFT-01 | `baseline` | none | Every acknowledged transaction reaches `Completed` or `Failed`; invariants hold. |
| CFT-02 | `kill-balance` | `docker kill` / `kubectl delete pod --grace-period=0 --force` of an `AccountBalance` replica, twice, during load | Kafka reassigns partitions, the survivor re-processes uncommitted records idempotently. |
| CFT-03 | `kill-ledger` | the same for a `CoreLedger` replica, which may die mid routing slip | The unfinished slip is redelivered and re-run; no duplicate and no orphan ledger entry. |
| CFT-04 | `kill-orchestrator` | the same for a `SagaOrchestrator` replica | The surviving replica continues the sagas from persisted state. |
| CFT-05 | `redis-latency` | Toxiproxy latency of 2.5 s ± 1.5 s on every Redis connection for 30 s | Lock acquisition slows down or fails and affected sagas end `Failed` with their hold released; no corruption. |
| CFT-06 | `redis-blackhole` | Toxiproxy `timeout` toxic (connections open, no bytes) for 20 s | Same as CFT-05 for the duration of the partition; the system recovers afterwards. |

Business rejections (insufficient funds, compliance rules) end a saga in `Failed` with a compensated hold; they are
part of the expected outcome mix and are not errors.

### Running

```bash
# Docker Compose: start the stack, scale is defined in docker-compose.yml
docker compose up -d --build
tests/chaos/scripts/run-chaos.sh baseline 2m
tests/chaos/scripts/run-chaos.sh kill-ledger 3m

# Redis scenarios route every Redis connection through Toxiproxy
REDIS_ENDPOINT=integration-bus-toxiproxy:6380 docker compose --profile chaos up -d
tests/chaos/scripts/run-chaos.sh redis-latency 2m

# Kubernetes (Kind)
helm upgrade --install integration-bus deploy/k8s/charts/integration-bus -n integration-bus --create-namespace \
  --set chaos.toxiproxy.enabled=true
BACKEND=k8s tests/chaos/scripts/run-chaos.sh kill-ledger 3m
```

`TARGET_RPS` (default `200`) and `ACCOUNT_COUNT` (default `300`) tune the load. Run artifacts are written to
`tests/chaos/work/<scenario>/` (ignored by git).

---

## 5. Consistency Invariants

`reconcile.sh` exits with a non-zero code if any invariant is violated.

| Invariant | Meaning |
| :--- | :--- |
| Accepted transactions equal sagas | Every HTTP `202` produced exactly one saga instance: nothing was dropped. |
| No saga in a non-terminal state | Every saga reached `Completed` or `Failed` after the drain period. |
| Journal sum equals the sum of deposits | Holds, releases and confirmations only move value: money is neither created nor destroyed. |
| No negative balance, no duplicate sequence number | The event-sourced journal stayed consistent under concurrent writers. |
| `Completed` saga: 1 hold, 2 confirmations, 0 releases, 1 ledger entry | The transfer was posted exactly once on both accounts and in the ledger. |
| `Failed` saga: 0 confirmations, 0 ledger entries, hold fully released | A failed transaction left no financial trace. |
| No journal or ledger record without a saga | No orphan records. |

---

## 6. Runbook

| Symptom | Action |
| :--- | :--- |
| Sagas remain in an `Awaiting*` state after the drain period | Check that all replicas are running (`docker compose ps`, `kubectl get pods`) and inspect the consumer lag in Kafka UI (`http://localhost:8080`). A replica that is down delays only its partitions until it restarts or the group rebalances. |
| `reconcile.sh` reports a count mismatch for accepted transactions | The k6 summary counts HTTP `202` responses. Re-run with the same `WORK_DIR` only after clearing `k6-summary.json`; the run window is delimited by the database clock, so concurrent traffic from another client inflates the saga count. |
| Redis scenarios abort with "Toxiproxy is not reachable" | Start the stack with the chaos profile and `REDIS_ENDPOINT` (Compose) or `chaos.toxiproxy.enabled=true` (Helm). |
