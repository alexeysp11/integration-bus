# 🛡️ Reliability Engineering: Distributed Locks & Rules Engine

[English](README.md) | [Русский](README.ru.md)

Describes the distributed-locking architecture (Redis / RedLock.net) around the balance critical section, and the
structure of the declarative JSON compliance rules engine (`RulesEngine`) used in
`IntegrationBus.AccountBalance.Service` and `IntegrationBus.Compliance.Service`.

---

## 1. The Problem Distributed Locks Solve

`HoldAccountBalanceConsumer` reads the account's current available balance (via
`IAccountStateReconstructor.ReconstructAvailableBalanceAsync`) and then — **within the same** database
transaction — inserts a hold entry into the journal. Because Npgsql defaults to the `Read Committed` isolation
level, and the sequence-based event-sourcing model does not lock the balance read with anything like
`SELECT ... FOR UPDATE`, if **two replicas** of `account-balance-service` process `HoldAccountBalance` for **the
same** account concurrently (Kafka does not guarantee both messages land on the same partition/replica, since
partitioning by `AccountId` is not yet implemented — see [`docs/roadmap.md`](../roadmap.md), Stage 6), a classic
TOCTOU (time-of-check-to-time-of-use) race is possible without additional synchronization:

1. Replica A reads the account balance: 100.
2. Replica B reads the same account's balance: 100 (before A has committed).
3. A reserves 80 — the check `100 >= 80` passes.
4. B reserves 80 — the check `100 >= 80` also passes (B has not yet seen A's change).
5. Result: 160 was debited from an account that had 100 — an overdraft.

The unique index `(SourceAccountId, SequenceNumber)` does not protect against this, because both replicas insert
entries with **different** `SequenceNumber` values — no uniqueness conflict occurs.

---

## 2. Solution: Redis Distributed Lock (RedLock.net)

### 2.1 Where It Is Wired Up
- Packages: `RedLock.net` + `StackExchange.Redis` (`IntegrationBus.AccountBalance.Service.csproj`).
- DI registration (`Program.cs`):
  ```csharp
  builder.Services.AddSingleton<IConnectionMultiplexer>(
      _ => ConnectionMultiplexer.Connect(redisConnectionString));

  builder.Services.AddSingleton<IDistributedLockFactory>(sp =>
      RedLockFactory.Create([new RedLockMultiplexer(sp.GetRequiredService<IConnectionMultiplexer>())]));
  ```
- Usage (`Consumers/HoldAccountBalanceConsumer.cs`): before opening the DB transaction, the consumer acquires a
  lock on the key `account-lock:{AccountFromId}`:
  ```csharp
  string lockResource = $"account-lock:{message.AccountFromId}";
  using IRedLock accountLock = await lockFactory.CreateLockAsync(
      lockResource, expiryTime: 15s, waitTime: 10s, retryTime: 200ms, cancellationToken);

  if (!accountLock.IsAcquired)
  {
      // publish HoldAccountBalanceFailed with an explicit reason -- fail-safe, not a silent hang
      return;
  }
  // ... the entire critical section (balance read + hold-entry insert) executes under the lock
  ```

### 2.2 Lock Parameters and Their Rationale
| Parameter | Value | Rationale |
|---|---|---|
| `expiryTime` | 15 seconds | The lock's TTL in Redis. If the lock-holding process crashes (pod crash) before explicitly releasing it, the lock automatically expires instead of blocking the account forever. |
| `waitTime` | 10 seconds | How long a waiting thread attempts to acquire the lock before giving up. |
| `retryTime` | 200 ms | The interval between acquisition attempts within `waitTime`. |

### 2.3 Deadlock Scenarios and Why There Are None Here
A classic distributed deadlock occurs when thread A holds a lock on resource `X` and waits for a lock on `Y`,
while thread B simultaneously holds a lock on `Y` and waits for `X`. In the current implementation,
`HoldAccountBalanceConsumer` acquires **exactly one** lock at a time (`account-lock:{AccountFromId}`) and never
attempts to acquire a second lock while holding the first — so a circular wait (the precondition for a deadlock)
is structurally impossible on this path.

**Important, deliberate limitation (not an oversight):** only `AccountFromId` (the account funds are debited
from) is locked, because that is the account whose available balance changes during the `Hold` step.
`ConfirmAccountBalanceConsumer`/`ReleaseAccountBalanceConsumer` do not take a Redis lock in the current version —
they are protected against duplicates by a separate idempotency mechanism (checking whether an entry with the
given `TransactionId` already exists before inserting, see §3), not by locking the account, since their work
relies on an already-created hold entry found by `TransactionId`, not on recomputing the balance from scratch.

### 2.4 What Happens When Redis Is Unavailable
In that case, `CreateLockAsync` does not throw an exception — it returns a lock with `IsAcquired == false`. The
consumer interprets this as an explicit, expected failure (fail-safe) and publishes `HoldAccountBalanceFailed`
with a clear reason, instead of hanging or, worse, proceeding without race protection (fail-open).
`AspNetCore.HealthChecks.Redis` on `GET /health` signals this degradation ahead of time, before it starts causing
transaction failures.

### 2.5 Deliberate Simplification: a Single Redis Instance, Not a Full Redlock Quorum
The Redlock algorithm, in its original formulation, is designed for **N ≥ 3 independent** Redis masters and
requires a majority quorum to guarantee safety under network partitions. `docker-compose.yml` runs a **single**
instance (`integration-bus-redis`, with `--appendonly yes` for persistence). For this project's purposes this is
a deliberate trade-off: the same `RedLock.net` client is used, but in effect this is a simple mutual-exclusion
lock over a single Redis instance — sufficient to eliminate the demonstrated race in a staging/single-node
environment. For production with higher availability requirements, the next step would be to run 3–5 independent
Redis instances and pass all of their addresses into `RedLockFactory.Create([...])`.

---

## 3. Idempotency Under Kafka Redelivery

`HoldAccountBalanceConsumer`, `ConfirmAccountBalanceConsumer`, `ReleaseAccountBalanceConsumer`, and
`TopUpAccountBalanceConsumer` are protected against Kafka's at-least-once redelivery: before inserting a new
journal entry, each consumer checks whether an entry with the given `TransactionId` and `EntryType` already
exists. On detecting a duplicate, the consumer commits an empty transaction and re-publishes the `...Passed`
event (an idempotent replay) without mutating the balance again.

---

## 4. Compliance Rules Engine: Declarative JSON Rules

The `RulesEngine` package (Microsoft, NuGet ID `RulesEngine`, v6.0.1) loads a workflow from
`src/Services/IntegrationBus.Compliance.Service/Rules/compliance-rules.json` once at service startup
(`ComplianceRulesEvaluator`, registered as a singleton), and on every `CheckComplianceLimits` runs
`ExecuteAllRulesAsync` over a flat projection of the message (`ComplianceRuleInput`). The first violated rule
becomes the failure reason (`ComplianceEvaluationResult.FailureReason`), which is written to
`ComplianceAudits.FailureReason` and propagated into `CheckComplianceLimitsFailed.Reason`.

### 4.1 Rule File Structure
```json
[
  {
    "WorkflowName": "TransactionComplianceWorkflow",
    "Rules": [
      {
        "RuleName": "PositiveTransactionAmount",
        "ErrorMessage": "Transaction amount must be strictly greater than zero.",
        "Expression": "input.Amount > 0",
        "RuleExpressionType": "LambdaExpression"
      }
    ]
  }
]
```
- **`WorkflowName`** — the workflow name passed to `ExecuteAllRulesAsync(workflowName, ...)`; must match the
  `WorkflowName` constant in `ComplianceRulesEvaluator.cs`.
- **`Rules[].RuleName`** — a unique rule name, surfaced in the log and in `FailureReason` as a fallback if
  `ErrorMessage` is not set.
- **`Rules[].Expression`** — a C#-like lambda expression compiled by `RulesEngine` into a real delegate; the
  `input` parameter is the `ComplianceRuleInput` instance, passed as `new RuleParameter("input", ruleInput)`.
- **`Rules[].ErrorMessage`** — a human-readable failure reason, returned as-is.
- **`Rules[].RuleExpressionType`** — `LambdaExpression` for every rule in this file.

Currently 4 rules are configured: a positive amount, a single-transaction maximum (1,000,000), distinct
sender/receiver accounts, and a valid currency code range (`Currency` enum, 1–11).

### 4.2 Adding a New Rule
1. Add an object to the `Rules` array in `compliance-rules.json` (the file is copied to the build output
   directory — `CopyToOutputDirectory: PreserveNewest`, the path is configured via
   `ComplianceRules:RulesFilePath` in `appsettings.json`, defaulting to `Rules/compliance-rules.json`).
2. If the rule needs a new message field, add it to `ComplianceRuleInput` and to the mapping inside
   `CheckComplianceLimitsConsumer.Consume`.
3. **Known limitation:** rules are loaded once at service startup (singleton) — there is no hot-reload of the
   file at runtime; after editing `compliance-rules.json` the service must be restarted
   (`docker compose restart integration-bus-compliance-service`).

### 4.3 Tests
`tests/IntegrationBus.Compliance.Service.Tests/Rules/ComplianceRulesEvaluatorTests.cs` links in **the actual**
production rules file (via an MSBuild `<None Include="..." Link="...">`, not a copy), so a regression in the
JSON file itself is caught by tests rather than only at runtime.
