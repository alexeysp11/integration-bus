# Chaos Engineering & Fault Tolerance Testing Strategy

Distributed financial high-load applications must guarantee eventual consistency and correct state transitions even during partial infrastructure collapse. This document outlines the strategies for embedding automated resilience validation into our MassTransit-orchestrated environment.

---

## 1. Core Testing Objectives
* **Saga Resilience:** Validate that `TransactionSagaStateMachine` correctly triggers compensating events if a downstream microservice experiences unexpected downtime.
* **Courier Routing Slip Recovery:** Verify that local multi-storage technical rollbacks inside `CoreLedger.Service` clean up partially written states (PostgreSQL/Redis) when late-stage activities timeout.
* **Network Partition Survivability:** Ensure that message delivery failures between Kafka/RabbitMQ and consumers do not result in orphaned financial transactions or double-spending.

---

## 2. Approach 1: Deterministic Fault Injection via DI (Scrutor Decorators)
To test complex error states reliably within automation pipelines (CI/CD), we intercept and corrupt internal service execution flows before relying on heavy infrastructure tools.

### Implementation Blueprint
Using **Scrutor**, we dynamically wrap MassTransit consumers or domain services inside proxy decorators. These decorators actively listen to inbound message headers or specific payloads to deterministically throw exceptions.

```csharp
// Example of a Chaos Decorator for the Balance Holder step
public sealed class ChaosHoldAccountBalanceConsumerDecorator : IConsumer<HoldAccountBalance>
{
    private readonly IConsumer<HoldAccountBalance> _innerConsumer;

    public ChaosHoldAccountBalanceConsumerDecorator(IConsumer<HoldAccountBalance> innerConsumer)
    {
        _innerConsumer = innerConsumer;
    }

    public async Task Consume(ConsumeContext<HoldAccountBalance> context)
    {
        // Intercept headers injected by k6 or Integration Tests
        if (context.Headers.TryGet("X-Chaos-Trigger", out string trigger) && trigger == "Timeout")
        {
            throw new TimeoutException("Chaos Engineering: Simulated database timeout.");
        }

        await _innerConsumer.Consume(context);
    }
}
```

### Key Advantages
* **Perfect for CI/CD:** Zero external dependencies required to execute failure paths.
* **Granular Control:** Allows testing precise edge cases (e.g., failure exactly after writing to Postgres but before clearing Redis cache).

---

## 3. Approach 2: Infrastructure-Level Chaos Engineering (Data-Driven Chaos)
While deterministic injection tests the code logic, infrastructure chaos tests the system's operational topology under stress. We combine heavy load injection with runtime degradation.

### Tooling Strategy
1. **Toxiproxy (Network Layer):** Simulates high network latency, jitter, or total connection drops between our services and external dependencies (Kafka, Redis, PostgreSQL).
2. **Chaos Mesh / LitmusChaos (Orchestration Layer):** Automatically terminates specific Kubernetes pods running consumer instances during active high-throughput workloads.

### Target Test Matrix
* **Database Blinking:** Induce 5-second connection drops to the Ledger DB while `k6` is driving 2,000 RPS. System must queue messages via MassTransit Outbox and process them post-recovery.
* **Broker Unavailability:** Drop connection to Kafka mid-Saga. The `SagaOrchestrator` must maintain its persisted state in the Saga database and resume tracking when the broker recovers.

---

## 4. Minimum Viable Test Scenarios

| Scenario ID | Test Name | Injected Failure | Expected System Behavior |
| :--- | :--- | :--- | :--- |
| **CFT-01** | Happy Path Load | None (Baseline) | 100% transaction success rate under targeted baseline RPS. |
| **CFT-02** | Middle-Saga Business Reject | Compliance service returns validation failure | `TransactionSagaStateMachine` catches failure, triggers compensation, releases balance hold. Global state set to `Failed`. |
| **CFT-03** | Late Technical Crash | Redis timeout inside Courier Routing Slip | The Routing Slip executes compensating activities in reverse order (e.g., deletes audit log). Business state remains consistent. |
| **CFT-04** | Consumer Pod Kill | Hard termination of `CoreLedger` pod mid-write | MassTransit acknowledges message loss, Kafka/RabbitMQ re-delivers the message to a healthy replica. **Idempotency checks prevent duplicate writes.** |
