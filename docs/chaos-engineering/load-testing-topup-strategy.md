# Load Testing Execution Strategy: Continuous Balance Replenishment with k6

Distributed financial systems executing stateful transactions (Sagas) naturally deplete account balances during standard debit workflows. Without a strategy to replenish these funds, load testing scripts will trigger massive cascaded business logic failures (`Insufficient funds`) within minutes, rendering technical stress testing impossible. 

This document defines how `k6` interacts with the `POST /api/v1/accounts/{id}/topup` endpoint inside `Accounting.Service` to maintain steady-state system equilibrium.

---

## 1. Core Testing Paradigms

To simulate real-world financial traffic and achieve continuous, uninhibited throughput, `k6` utilizes two main load distribution patterns:

### Pattern A: Probability-Based Equilibrium (Continuous Flow)
Each Virtual User (VU) loop simulates a random user behavior profile based on pre-defined probability weight distribution. 
* **85% of iterations** trigger a debit transaction via `Processing.Api` (driving the multi-level Saga).
* **15% of iterations** trigger a balance top-up via `Accounting.Service` (bypassing the orchestrator to credit the balance).

#### Mathematical Equilibrium Rule
To ensure that the total money supply in the database remains stable, the Top-Up Amount must balance out the Debit Amount over time:

$$\text{Probability}_{\text{Top-Up}} \times \text{Amount}_{\text{Top-Up}} \approx \text{Probability}_{\text{Debit}} \times \text{Amount}_{\text{Debit}}$$

*Example:* If a standard debit transaction is $10.00, then the 15% top-up actions must inject a larger chunk (e.g., $56.66) to ensure the system never completely runs dry during multi-hour soak testing.

### Pattern B: Phased Stress & Recovery (Spike Simulation)
The test profile is divided into two distinct structural execution stages:
1. **The Depletion Phase (Black Friday Simulation):** `k6` aggressively bombards the system with debit sagas until the seeded account funds approach zero.
2. **The Recovery Phase (Top-Up Wave):** As soon as compliance/balance thresholds trigger business aborts, `k6` drops the saga load and unleashes maximum concurrency directly at the `/topup` endpoint. This verifies the write throughput limits of the modified ledger database.

---

## 2. k6 Reference Script Structure (Pattern A Implementation)

Below is the standard Javascript configuration blueprint required to run the continuous equilibrium test pattern in `k6`:

```javascript
import http from 'k6/http';
import { check, sleep } from 'k6';
import { randomIntBetween } from 'https://k6.io';

export const options = {
    stages: [
        { duration: '1m', target: 50 },  // Ramp-up to 50 concurrent users
        { duration: '5m', target: 50 },  // Stay at 50 users (Soak phase)
        { duration: '1m', target: 0 },   // Ramp-down
    ],
    thresholds: {
        http_req_failed: ['rate<0.01'],  // Less than 1% server errors allowed
        http_req_duration: ['p(95)<200'] // 95% of requests must complete under 200ms
    },
};

// Global variables containing the boundaries of pre-seeded test data pool
const TOTAL_SEEDED_ACCOUNTS = 100000;
const PROCESSING_API_URL = 'http://localhost:5000/api/v1/transactions';
const ACCOUNTING_API_URL = 'http://localhost:5001/api/v1/accounts';

export default function () {
    // Select a deterministic pseudo-random account ID from the pre-seeded database range
    const sourceAccountId = `00000000-0000-0000-0000-${String(randomIntBetween(1, TOTAL_SEEDED_ACCOUNTS)).padStart(12, '0')}`;
    const targetAccountId = `00000000-0000-0000-0000-${String(randomIntBetween(1, TOTAL_SEEDED_ACCOUNTS)).padStart(12, '0')}`;

    const roll = Math.random(); // Generate a distribution value between 0.0 and 1.0

    if (roll < 0.85) {
        // --- 85% Probability: Fire Global Stateful Saga ---
        const payload = JSON.stringify({
            sourceAccountId: sourceAccountId,
            targetAccountId: targetAccountId,
            amount: 10.00,
            currency: "USD"
        });

        const params = { headers: { 'Content-Type': 'application/json' } };
        const res = http.post(PROCESSING_API_URL, payload, params);

        check(res, {
            'Saga Accepted (202)': (r) => r.status === 202,
        });
    } else {
        // --- 15% Probability: Direct Account Top-Up (Equilibrium Trigger) ---
        const payload = JSON.stringify({
            amount: 60.00 // Higher value to compensate for lower execution frequency
        });

        const params = { headers: { 'Content-Type': 'application/json' } };
        const res = http.post(`${ACCOUNTING_API_URL}/${sourceAccountId}/topup`, payload, params);

        check(res, {
            'Top-Up Successful (200)': (r) => r.status === 200,
        });
    }

    // Pacing delay between iterations per Virtual User to model realistic transaction arrival rate
    sleep(0.1); 
}
```

---

## 3. Operational Requirements for Load Ingestion

1. **Independent Network Channels:** In high-concurrency environments, `k6` should ideally hit the target through separate ingress routing endpoints or gateway ports to avoid artificial HTTP/gRPC connection pool bottlenecks between `Processing.Api` and `Accounting.Service`.
2. **Log Silencing:** During a 2000+ RPS execution run, console logging inside the `/topup` controller must be set to `LogLevel.Warning` or higher. Chatty `LogInformation` outputs will cause heavy CPU-bound disk IO blocking, falsifying latency telemetry results.
