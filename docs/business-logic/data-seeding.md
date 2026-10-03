# Bulk Test Account Seeding

The `AccountBalance.Service` database uses an event-sourced balance model, so a freshly seeded account always
starts at a balance of `0` — there is no `initialBalance` field. This endpoint exists purely to populate the
`Accounts` table with a large pool of valid account identifiers for manual testing and load-testing scenarios; use
the top-up endpoint (see [`api-specifications.md`](api-specifications.md)) afterward to fund any account you plan
to debit from.

---

## 1. Environment Gating

* The route is registered with a `[DenyProductionEnvironment]` filter and returns `HTTP 404 Not Found` whenever
  `ASPNETCORE_ENVIRONMENT=Production`, regardless of request content.
* Available under all other environment profiles (`Development`, `Testing`, etc.).

---

## 2. API Contract

* **Route:** `POST /api/v1/accounts/seed`
* **Content-Type:** `application/json`

#### Request Payload
```json
{
  "count": 100000,
  "currency": 1
}
```
* `count`: number of accounts to generate (default `100000` if omitted).
* `currency`: a defined, non-`None` value of the `Currency` enum (see [`api-specifications.md`](api-specifications.md)).

#### Success Response
`HTTP 202 Accepted`, empty body. The accounts are generated asynchronously; poll the database directly to confirm
completion (see [`validation-guide.md`](validation-guide.md) §2 for the exact `psql` query).

#### Error Response
`HTTP 400 Bad Request` if `count` is zero or negative.

---

## 3. Processing Model

The request is published as a `SeedAccountDatabaseBulkData` command onto Kafka and consumed asynchronously by
`AccountBalance.Service`'s `SeedAccountDatabaseBulkDataConsumer`:

* Generated in batches of 10,000 via `DbContext.AddRangeAsync` + `SaveChangesAsync`, with change tracking disabled
  (`AutoDetectChangesEnabled = false`, `QueryTrackingBehavior.NoTracking`) to keep memory flat regardless of the
  requested count.
* Each account is assigned a random historical `CreatedAt` timestamp within the last 365 days, so seeded data looks
  realistic in time-series dashboards.
* On completion, a `SeedAccountDatabaseBulkDataPassed` event (carrying the seeded quantity) is published back onto
  Kafka.
