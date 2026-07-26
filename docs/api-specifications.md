# API Specifications, Contracts, and Validation Rules

This document serves as the single source of truth for all public HTTP endpoints exposed by the API Gateway (`IntegrationBus.Gateway`). It defines route topologies, payload schemas, strict inbound validation rules, and manual verification procedures.

---

## 1. Global Architectural Standards

* **Strict Input Validation:** All inbound payloads are intercepted at the gateway/processing boundary via **FluentValidation**. Any breach of constraints short-circuits the pipeline, returning an `RFC 7807 Problem Details` (HTTP 400 Bad Request) block.
* **Asynchronous Ingestion:** High-throughput mutate endpoints return `HTTP 202 Accepted` immediately upon streaming data into Kafka, shifting transaction processing to an asynchronous worker lifecycle.

---

## 2. HTTP Endpoint Contracts

### 📌 Endpoint A: Execute Distributed Transaction
* **Route:** `POST /api/ledger/transaction`
* **Content-Type:** `application/json`

#### Request Payload Example
```json
{
  "transactionId": "b1111111-2222-3333-4444-999999999977",
  "sourceAccountId": "a2222222-3333-4444-5555-999999999999",
  "targetAccountId": "c3333333-4444-5555-7777-777777777777",
  "amount": 100.00,
  "currency": 1
}
```

#### Validation Rules & Constraints
* `TransactionId`: Mandatory, must be a non-empty valid GUID v4.
* `SourceAccountId`: Mandatory, must be a valid GUID. **Must not match `TargetAccountId`**.
* `TargetAccountId`: Mandatory, must be a valid GUID. **Must not match `SourceAccountId`**.
* `Amount`: Must be strictly greater than `0.00` with a maximum scale precision of 4 decimal places.
* `Currency`: Must map to a valid internally supported system asset enum integer.

#### Success Response (`202 Accepted`)
```json
{
  "message": "Transaction request received and distributed saga orchestration initiated.",
  "transactionId": "b1111111-2222-3333-4444-999999999977"
}
```

---

### 📌 Endpoint B: Account Balance Replenishment
* **Route:** `POST /api/v1/accounts/{id}/topup`
* **Content-Type:** `application/json`
* **Route Parameter:** `{id}` — Valid target account GUID.

#### Request Payload Example
```json
{
  "amount": 250.00,
  "currency": "USD"
}
```

#### Validation Rules & Constraints
* `id` (Route): Mandatory, must be a structurally sound GUID v4 identifier.
* `Amount` (Body): Must be strictly greater than `0.00` and fall under the single-operation velocity cap ($10,000,000.00). Max 4 decimal precision.
* `Currency` (Body): Mandatory, must be a 3-character uppercase ISO 4217 string code.

#### Success Response (`202 Accepted`)
```json
{
  "message": "Top-up request accepted and is being processed asynchronously.",
  "trackingTransactionId": "9f5b61e2-411a-4c22-990a-c8e6b12a5db3"
}
```

---

## 3. Manual Testing & Postman Verification

Before spinning up `k6` infrastructure profiles, individual system verification can be performed manually via Postman or `cURL`.

### cURL Execution Examples

#### 1. Dispatching a Valid Transaction Saga
```bash
curl -X POST http://localhost:5000/api/ledger/transaction \
  -H "Content-Type: application/json" \
  -d '{
    "transactionId": "b1111111-2222-3333-4444-999999999977",
    "sourceAccountId": "a2222222-3333-4444-5555-999999999999",
    "targetAccountId": "c3333333-4444-5555-7777-777777777777",
    "amount": 100.00,
    "currency": 1
  }'
```

#### 2. Triggering a Malformed Input Interception (Validation Test)
```bash
curl -X POST http://localhost:5000/api/v1/accounts/invalid-guid/topup \
  -H "Content-Type: application/json" \
  -d '{
    "amount": -50.00,
    "currency": "usd"
  }'
```
*Expected Outcome:* The API Gateway routing engine rejects the operation at the edge, returning `HTTP 400 Bad Request` with an RFC 7807 structure listing exact field constraint failures (`id` formatting, negative amount, lowercase currency).
