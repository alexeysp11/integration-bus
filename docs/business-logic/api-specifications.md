# API Specifications, Contracts, and Validation Rules

This document is the single source of truth for the public HTTP endpoints exposed by `IntegrationBus.Processing.Api`
(reachable directly, or through `IntegrationBus.Gateway.Api`'s YARP reverse proxy). It defines route topologies,
payload schemas, inbound validation rules, and manual verification procedures.

---

## 1. Global Architectural Standards

* **Strict Input Validation:** All inbound payloads are validated via **FluentValidation**. Any constraint
  violation short-circuits the pipeline and returns a flat `HTTP 400 Bad Request` body (see §3 below — **not**
  RFC 7807 Problem Details).
* **Asynchronous Ingestion:** Mutating endpoints return `HTTP 202 Accepted` immediately after publishing the
  corresponding command onto Kafka; the actual business processing happens asynchronously inside the distributed
  Saga (see [`docs/roadmap.md`](../roadmap.md) for the full orchestration design).
* **API Versioning:** All routes are exposed under `/api/v{version}/...` via `Asp.Versioning`; the current version
  is `v1`.

---

## 2. Interactive API Documentation via Scalar

`IntegrationBus.Processing.Api` exposes interactive, schema-driven API documentation via **Scalar** when running
under the `Development` environment profile:

* **Scalar UI:** `http://localhost:5201/scalar/v1`

---

## 3. HTTP Endpoint Contracts

### 📌 Endpoint A: Start a Distributed Transaction
* **Route:** `POST /api/v1/ledger/transaction` (also reachable via the gateway as `POST /api/ledger/transaction`)
* **Content-Type:** `application/json`

#### Request Payload
```json
{
  "sourceAccountId": "a2222222-3333-4444-5555-999999999999",
  "targetAccountId": "c3333333-4444-5555-7777-777777777777",
  "amount": 100.00,
  "currency": 1
}
```

#### Validation Rules
* `sourceAccountId`: required GUID.
* `targetAccountId`: required GUID, must not equal `sourceAccountId`.
* `amount`: must be strictly greater than `0.00`, maximum 4 decimal places.
* `currency`: must be a defined, non-`None` value of the `Currency` enum (`1` = USD, `2` = EUR, `3` = CHF, `4` = AUD, `5` = CAD, `6` = AED, `7` = GEL, `8` = JPY, `9` = CNY, `10` = RUB, `11` = BYN).

#### Success Response (`202 Accepted`)
```json
{
  "transactionId": "b1111111-2222-3333-4444-999999999977",
  "status": "Processing",
  "message": "Your transaction payload has been accepted and queued for processing."
}
```

`transactionId` is generated server-side and is not supplied by the caller. There is no endpoint to poll saga
status over HTTP — see [`validation-guide.md`](validation-guide.md) §3 for how to inspect the saga's persisted
state directly for black-box verification.

---

### 📌 Endpoint B: Account Balance Top-Up
* **Route:** `POST /api/v1/accounts/{id}/topup` (also reachable via the gateway as `POST /api/accounts/{id}/topup`)
* **Content-Type:** `application/json`
* **Route Parameter:** `id` — target account GUID.

#### Request Payload
```json
{
  "amount": 250.00,
  "currency": 1
}
```

#### Validation Rules
* `amount`: must be strictly greater than `0.00`, maximum 4 decimal places.
* `currency`: must be a defined, non-`None` value of the `Currency` enum (see Endpoint A).

#### Success Response (`202 Accepted`)
```json
{
  "trackingTransactionId": "9f5b61e2-411a-4c22-990a-c8e6b12a5db3",
  "message": "Top-up request accepted and is being processed asynchronously."
}
```

---

### 📌 Endpoint C: Bulk Seed Test Accounts
* **Route:** `POST /api/v1/accounts/seed`
* **Content-Type:** `application/json`
* **Availability:** unreachable (`HTTP 404`) when `ASPNETCORE_ENVIRONMENT=Production`.

See [`data-seeding.md`](data-seeding.md) for the full contract and environment-gating details.

---

## 4. Error Response Shape

A validation failure returns `HTTP 400` with a flat body:
```json
{
  "error": "Transaction amount must be a positive value strictly greater than zero."
}
```

---

## 5. Manual Testing via cURL

```bash
# Dispatch a valid transaction
curl -X POST http://localhost:5038/api/v1/ledger/transaction \
  -H "Content-Type: application/json" \
  -d '{
    "sourceAccountId": "a2222222-3333-4444-5555-999999999999",
    "targetAccountId": "c3333333-4444-5555-7777-777777777777",
    "amount": 100.00,
    "currency": 1
  }'

# Trigger a validation failure (same source/target account)
curl -X POST http://localhost:5038/api/v1/ledger/transaction \
  -H "Content-Type: application/json" \
  -d '{
    "sourceAccountId": "a2222222-3333-4444-5555-999999999999",
    "targetAccountId": "a2222222-3333-4444-5555-999999999999",
    "amount": 100.00,
    "currency": 1
  }'
# Expected: HTTP 400, {"error": "Target account identifier cannot match the source account identifier."}
```

For the complete end-to-end scenario (seed → top-up → transaction → saga verification → analytics), see
[`validation-guide.md`](validation-guide.md).
