# Automated Test Data Seeding Strategy

To execute high-throughput performance testing via `k6`, the system requires a baseline pool of 100,000 to 500,000 unique, valid accounts within the `Accounting.Service` database. This document defines the secure, environment-gated API seeding mechanism.

---

## 1. Architectural and Security Constraints

* **Strict Environment Isolation:** The seeding endpoint must never be registered or reachable in the Production environment. It is compiled/routed exclusively under `Development` or `Testing` profiles.
* **In-Memory Routing Protection:** Rather than relying on soft runtime authorization checks, the route is completely omitted from the ASP.NET Core routing table on production builds, returning a hard `404 Not Found`.
* **High-Throughput Ingestion:** To prevent HTTP connection timeouts when generating 500k rows, the endpoint bypasses EF Core's Change Tracker and leverages high-speed bulk utilities (e.g., `NpgsqlCopyHelper` or Dapper batch writes).

---

## 2. API Contract Specification

### Seed Test Accounts
* **Endpoint:** `POST /api/v1/accounts/seed`
* **Content-Type:** `application/json`
* **Availability:** `Development` / `Testing` environments only.

#### Request Payload
```json
{
  "count": 100000,
  "initialBalance": 10000.00,
  "currency": "USD"
}
```

#### Success Response (`202 Accepted`)
```json
{
  "message": "Bulk seeding operation initiated successfully.",
  "recordsRequested": 100000,
  "status": "Completed"
}
```

---

## 3. Reference Implementation Blueprint

The following snippet demonstrates how the endpoint is securely encapsulated and registered based on active environment variables inside `Program.cs`:

```csharp
public static class TestingEndpointsExtensions
{
    public static WebApplication MapTestingEndpoints(this WebApplication app)
    {
        var isTesting = app.Environment.IsDevelopment() || 
                        string.Equals(Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT"), "Testing", StringComparison.OrdinalIgnoreCase);

        if (!isTesting)
        {
            // Do not register endpoints under production profiles
            return app;
        }

        app.MapPost("/api/v1/accounts/seed", async (
            [FromBody] SeedAccountsRequest request,
            [FromServices] IBulkSeedingService seedingService,
            CancellationToken ct) =>
        {
            if (request.Count <= 0 || request.Count > 500000)
            {
                return Results.BadRequest(new { Error = "Count must be between 1 and 500,000." });
            }

            await seedingService.ExecuteBulkSeedAsync(request.Count, request.InitialBalance, request.Currency, ct);
            
            return Results.Accepted(value: new {
                Message = "Bulk seeding operation completed successfully.",
                RecordsRequested = request.Count,
                Status = "Completed"
            });
        });

        return app;
    }
}
```
