# 🗺️ `integration-bus` Evolution Roadmap: Toward a Fintech Platform

[English](integration-bus-evolution-roadmap.md) | [Русский](integration-bus-evolution-roadmap.ru.md)

## Sequencing Principle

Each subsequent task **builds on** the result of all previous ones — none is pulled forward "because it's small" if it has a real technological dependency. The three phases are arranged in the order explicitly requested by the stakeholder:

- **Phase 1 — Infrastructure and architectural foundation.** None of this is directly visible to the end business user, but without this foundation Phases 2 and 3 are either impossible or would require expensive retroactive refactoring (database schema migrations, threading multi-tenancy through already-written code, and so on).
- **Phase 2 — In-house business logic.** Everything that can be fully written and tested locally, without API keys, without third-party accounts, without network calls out (other than to our own Docker services like Gotenberg/MinIO).
- **Phase 3 — External integrations.** Everything that requires a real (even if test/sandbox) account with an external provider or region-specific infrastructure.

  > 🏛️ **Phase 3 architectural manifesto:** To keep the project a globally-applicable System Design solution, we strictly separate business logic from vendors. All external systems are hidden behind abstractions (the **Strategy** and **Adapter** patterns). To guarantee **100% test-environment (sandbox) autonomy**, the international connectors use open-source emulators running locally on the Docker network, which guarantees the infrastructure works from anywhere in the world without a VPN.

  #### Map of architectural abstractions and test-environment providers:

  | Business domain (interface) | Local sandbox for Docker (100% available) | CIS/Russia provider (production) | International provider (production) | Architectural role in the bus |
  | :--- | :--- | :--- | :--- | :--- |
  | **IPaymentGateway** | `stripe/stripe-cli` *(local container)* | YuKassa | Stripe / Adyen | Processing of distributed transactions, payment idempotency, and webhooks. |
  | **IFiscalFormat** | `Mock-Fiscal-Server` *(internal stub)* | ATOL (Russian Federal Law 54-FZ) | TaxJar / Avalara | Asynchronous receipt fiscalization, queue management to the fiscal register. |
  | **IAddressValidator** | `OpenStreetMap / Nominatim` *(self-hosted)* | DaData | Google Places / Loqate | Cleansing, validation, and normalization of counterparty master data on import. |
  | **IFxRatesProvider** | `CBR.ru API / Open Exchange Rates` | Bank of Russia rates | Bloomberg / Fixer.io | ETL/CDC import of exchange-rate matrices for multi-currency processing. |
  | **IEdiAdapter** | `OpenPeppol Validator / Mock EDI` | Kontur.Diadoc | SAP Ariba / Peppol | Legally binding electronic document interchange (B2B EDI) and document signing. |
  | **ICryptoEngine** | `OpenSSL (X.509) / PKCS#11 container` | CryptoPro (GOST) | AWS CloudHSM / Vault | Document-signing module. Supports both standard and national GOST algorithms. |
  | **IIdentityProvider** | `Keycloak` *(local instance)* | VK OAuth / Yandex ID | Google / GitHub Auth | Federated user authentication via OIDC / OAuth 2.0. |


Task numbering is continuous (1…40) so the overall execution order is explicit. Every evolution task is tagged with its dependencies ("depends on task #…").

---

## 📦 Phase 1. Infrastructure and Architectural Foundation

### Task 1. CI Markdown Link Validator (Lychee)
**Dependencies:** none — a fully independent task, a good "warm-up".

- We choose **Lychee** (Rust) — compared with `markdown-link-check` (Node.js) and `muffet` (Go), it is faster and handles relative `.md` paths better, which matters: we already have a dozen cross-links between `docs/*.md` files (`README.md` → `docs/roadmap.md` → `docs/observability/README.md`, etc.) accumulated over this session.
- `.lychee.toml` config at the repository root: exclude `http://localhost:*`, `http://integration-bus-*` (internal docker hostnames that appear in `docker-compose.yml` examples inside `docs/`), 8s timeout.
- `.github/workflows/markdown-link-checker.yml` — a separate workflow (we don't touch the existing `ci.yml` from Stream 6), triggered on `pull_request`/`push` to `main`, `fail-fast` on a broken link, `GITHUB_TOKEN` in the env to avoid rate limits on `github.com` links.
- Document the local run command in `README.md` under a new "For Developers" section.

*Why first:* zero risk, doesn't touch service code, immediately raises the quality of the documentation we're about to actively grow (EDM, FX, billing — all new `docs/*.md` files), before the links have a chance to rot.

---

### Task 2. CQRS + MediatR — architectural split of Write/Read circuits
**Dependencies:** none hard, but **everything written in Phases 2–3 from here on should be designed as `IRequest`/`IRequestHandler` from the start**, otherwise 20+ new features will need to be rewritten.

- Add `MediatR` to `IntegrationBus.Shared` (or a new `IntegrationBus.Cqrs` library), with three cross-cutting `IPipelineBehavior`s:
  - `ValidationBehavior` — runs through the `FluentValidation` already in use (already present in `Gateway.Api`/`Processing.Api` per [`docs/roadmap.md`](roadmap.md) → "API Governance").
  - `IdempotencyBehavior` — `X-Idempotency-Key` in Redis (Redis is already running and used by RedLock — we reuse the same `IConnectionMultiplexer`).
  - `LoggingBehavior` — through the existing Serilog/`ILogger<T>` pipeline (the very one we just fixed for `SagaOrchestrator.Service`).
- **Important:** the existing saga and Courier Routing Slip are **left untouched** — MediatR does not replace the MassTransit Saga State Machine here; it closes off a new circuit (`Management.API`, future billing, the DLQ manager) where command logic is simpler than a distributed saga.
- All Phase 2/3 commands (DLQ resend, counterparty limit changes, manual clearing, billing commands) are written through `IRequest<TResponse>` from day one.

*Why second:* putting the architectural skeleton in place before the first real UI/business use case appears is the only way to avoid rewriting 15 features later. Task 1 creates no dependencies, so the 1→2 order is arbitrary, but 2 must come before almost everything else.

---

### Task 3. Standardizing the message envelope — CloudEvents
**Dependencies:** task 2 (new commands/events are more convenient to wrap in a single envelope from the start).

- `CloudNative.CloudEvents.SystemTextJson` — wrap **new** contracts (FX, clearing, anti-fraud, billing from Phase 2/3) in a `CloudEvent` envelope with metadata (`type`, `source`, `time`, `subject`).
- **Existing** MassTransit contracts (`HoldAccountBalance`, `WriteLedgerRecord`, etc.) are **not** rewritten — that would break the compatibility of an already-tested saga for no benefit. CloudEvents is introduced only for new flows where external (EDM/ATOL/YuKassa) or cross-team compatibility actually matters.

*Why third:* this needs to happen **before**, not after, a dozen new event types (FX, clearing, anti-fraud) appear — otherwise the events themselves would need rewriting.

---

### Task 4. Mapperly — replacing manual mapping
**Dependencies:** task 2 (mapping mostly lives inside an `IRequestHandler`).

- Add `Mapperly` to the new services (DLQ Inspector, Billing, FX) for Command → Entity → Event transformation.
- The existing consumers (`ConfirmAccountBalanceConsumer`, etc.) are left untouched — the mapping there is trivial and explicit; Mapperly adds no value there, and there is a regression risk.

---

### Task 5. Hangfire — background job engine
**Dependencies:** task 2 (retry/cleanup commands are also `IRequest`), uses the existing Postgres (a separate `hangfire_db` database in `docker-compose.yml`, following the pattern of `saga_db`/`accounting_db`).

- **DLQ Retry Engine:** exponential backoff+jitter (1 min → 5 min → 30 min) for messages not delivered to external circuits. This is the technical foundation for the future DLQ Inspector (task 22) and for the resilience of Phase 3's external integrations (YuKassa/EDM/ATOL will inevitably be unavailable sometimes).
- **Housekeeping:** a periodic job that cleans up logs of successfully delivered messages older than 30 days — applied to already-existing tables (`JournalEntries`, `OutboxMessage`), carefully, so as not to interfere with the `non_replicated_deduplication_window` logic of the already-verified ClickHouse pipeline.
- The Hangfire Dashboard is stood up behind Basic Auth inside `Management.API` (task 11) — another reason Management.API comes later.

*Why fifth:* almost every Phase 2/3 feature (scheduled clearing, email dispatch, ATOL receipt-status polling, EDM polling) needs a background job scheduler — better to set it up once, now.

---

### Task 6. Multi-tenancy — database architectural foundation
**Dependencies:** none technical, but **this is the last point at which it can be introduced cheaply** — before 10+ new tables appear (FX accounts, clearing, disputes, billing, EDM documents) in Phase 2/3.

- *Logical Tenant Separation* pattern: a `TenantId` column on every new table, global EF Core query filters.
- **Important and deliberate:** the existing tables (`Accounts`, `JournalEntries`, `TransactionState`, `LedgerEntries`) are **not** migrated right now — our current system runs in single-tenant ("our own company") mode, and an aggressive migration carries no immediate business value. The right strategy is to introduce `TenantId` as a **convention for all new code**, and to make retroactive migration of existing tables a separate, explicitly estimated technical-debt ticket once a real second tenant actually appears.

*Why right here:* doing this after FX/clearing/billing (which are inherently multi-tenant-ready SaaS features by nature) would be an order of magnitude more expensive.

---

### Task 7. Rate Limiting + Circuit Breaker — ingress/egress protection
**Dependencies:** none direct, but logically continues the "infrastructure hardening" of tasks 5–6.

- **Ingress:** `Microsoft.AspNetCore.RateLimiting` (or a Redis token-bucket via `AspNetCore.RateLimiting.Redis` — we already have a Redis cluster from RedLock, so the second option is preferable for horizontal scaling) on `Gateway.Api`/the future `Management.API`.
- **Egress:** `Polly.Core` Circuit Breaker on HttpClient pipelines — direct preparation for Phase 3: once real calls to YuKassa/ATOL/Diadoc/DaData appear, they must go through an already-ready Circuit Breaker rather than a bare `HttpClient`.

*Why here:* this must be ready **before** the first outbound external HTTP call (Phase 3), so it's logical to introduce it right after the database foundation (task 6), while nothing is yet tied to specific external providers.

---

### Task 8. Resilience & HA — hardening the existing infrastructure
**Dependencies:** task 6 (multi-tenancy is already baked into the schema — no need to redo it twice when moving to a cluster), logically follows the "protective" tasks 5-7.

- **Postgres → Patroni cluster** (1 master + 2 sync replicas) — upgrading our `*_db` databases (`saga_db`, `accounting_db`, `compliance_db`, `ledger_db`) without changing the connection-string convention (applications keep knowing about only one logical host — Patroni/HAProxy hides the topology).
- **Redis → Sentinel/Cluster** — critical, since Redis already carries RedLock (distributed locks), the future Idempotency Behavior (task 2), and Rate Limiting (task 7) — losing Redis today would take down three subsystems at once.
- **Bulkhead Isolation** via MassTransit/Polly — isolating future integration channels (SBP/YuKassa/crypto) into separate thread pools, so a failure in one external provider (Phase 3) doesn't take down the rest.
- Transactional Outbox **already exists** (confirmed by the `OutboxResilienceTests` test) — here we only document it and explicitly tie it to the HA strategy.

*Why here and not at the very start:* there is no point clustering the database **before** the final schema is settled (multi-tenancy, task 6) — otherwise migrations would have to be run against the cluster twice.

---

### Task 9. Deep Observability — business metrics and health circuit
**Dependencies:** tasks 5–8 (new components appeared — Hangfire, Patroni, Redis Cluster — that need to be wired into the already-existing Prometheus/Grafana).

- **New business metrics** via `System.Diagnostics.Metrics.Meter`: `integration_bus_transactions_total`, `integration_bus_processing_duration_seconds`, `integration_bus_financial_volume` — extending the already-working `/metrics` endpoint (present in every service) with new counters, without touching the existing .NET runtime/MassTransit metrics.
- **Extended `/health`:** `AspNetCore.Diagnostics.HealthChecks` with Redis/Kafka/Postgres checks (partially already in use — `AspNetCore.HealthChecks.NpgSql`/`.Kafka`/`.Redis` are already present in `CoreLedger.Service.csproj`) + adding an S3 health check (once it exists, Phase 2 task 11) as dependencies become ready.
- Distributed tracing **is already implemented and verified** (a single `TraceId` across 6 services, see [`docs/observability/README.md`](observability/README.md) §5) — here we only make sure the new components (Hangfire jobs, Management.API) also appear in the trace via `ActivitySource`.

---

### Task 10. .NET Aspire — orchestration for development
**Dependencies:** task 9 — Aspire duplicates/complements what's already assembled in Prometheus/Grafana/Jaeger; it makes sense to introduce it once baseline observability is stable.

- The Aspire AppHost wraps the **existing** `docker-compose.yml` (rather than replacing it — this task is not about migrating infrastructure, it's about a dev-time dashboard). For local development, the Aspire Dashboard gives a single screen "message came in → where it's stuck → where's the error" without switching between Grafana/Jaeger/Kafka UI.
- Not critical for production (there the Prometheus+Loki+Jaeger combination is in place), but sharply speeds up onboarding new developers onto the remaining 30 tasks of this plan.

---

### Task 11. Management.API + SPA shell
**Dependencies:** task 2 (CQRS — Management.API is built **entirely** as a thin HTTP layer over `IMediator.Send(...)`), task 7 (Rate Limiting on the new public API), task 9 (health circuit for the new service).

- A new `IntegrationBus.Management.Api` project — **architecturally isolated** from `IntegrationBus.*.Service` (key architectural principle: the frontend must never talk to `SagaOrchestrator`/Kafka directly). It works against service databases, audit logs, and rule configuration — i.e. tables that appear in subsequent tasks (limits already partly exist via the RulesEngine JSON, but without a UI).
- The `integration-bus-dashboard/` SPA in React + TypeScript + Vite, structured as `features/{dlq-inspector,mapper,routing,finance}/`, `services/` (the Management.API client).
- At this stage — **only the shell and the first finished page** (e.g. the Health/Status dashboard, since its backend is already fully ready — task 9). The remaining panels (DLQ Inspector, Mapping Designer, Reconciliation Dashboard, Routing Builder, Billing Matrix, E2E Tracer) are added **as the corresponding backend feature becomes ready** — see task 31 in Phase 2, where they get wired in one by one.

*Why right now and not earlier:* putting the frontend in place before the CQRS layer exists would mean either building controllers directly on top of MassTransit (architecturally forbidden by the source plan) or rewriting Management.API right after task 2.

---

### Task 12. DocFX — auto-generated knowledge base
**Dependencies:** task 3 (CloudEvents envelopes provide richer metadata for contract documentation).

- Generate documentation for all `IntegrationBus.*.Contracts` directly from the C# code (XML comments are already extensively present in the contracts — visible in `WriteLedgerRecordFailed.cs` and neighboring files). DocFX assembles an integration map for analysts automatically, with no manual duplication into `docs/*.md`.
- Publishing — a static site on GitHub Pages, following the same pattern as the Allure report publishing in task 14 (the same `gh-pages` branch, shared CI infrastructure).

---

### Task 13. QA/CI circuit — SonarCloud + Playwright + Allure
**Dependencies:** task 11 — Playwright tests the real UI, which didn't exist before Management.API; task 12 — shared infrastructure for publishing static reports (`gh-pages`).

- `SonarSource/sonarcloud-github-action` in the existing `.github/workflows/ci.yml` (existing Stream 6) — Quality Gate: ≥80% coverage on new code, 0 critical vulnerabilities, <3% duplication.
- A new `IntegrationBus.E2ETests` project (`Microsoft.Playwright.NUnit`) — the first scenarios: logging into Management.API → the health dashboard. Full scenarios (DLQ resend, limit configuration) are added as the corresponding panels appear (task 31).
- `Allure.Xunit` — tag the **already-existing** 7 test projects (119 tests) with attributes (`[AllureFeature]`/`[AllureStory]`), so there's a live report from day one instead of an empty one.

*Why this is the last Phase 1 task:* it logically closes out the infrastructure circuit — the entire foundation (CQRS, Hangfire, multi-tenancy, HA, Management.API) has to exist before there's anything to check with static analysis and E2E tests.

---

## 🧩 Phase 2. In-House Business Logic (No External Integrations)

### Task 14. Moving the ledger to double-entry + immutability
**Dependencies:** task 8 (HA — it's more convenient to rewrite the ledger schema on an already-stabilized DB topology), task 6 (multi-tenancy — the new schema is designed tenant-aware from the start).

This is the **most fundamental** business task in the whole plan — almost every subsequent fintech feature (FX, clearing, disputes, billing) requires true double-entry accounting, not the current `LedgerEntries(TransactionId, Amount, CreatedAt)` in `CoreLedger.Service`.

- **Double-Entry Bookkeeping:** every operation is at least 2 postings (Debit/Credit), `Σ Debit = Σ Credit`. We extend `LedgerEntryEntity` (or introduce a new `LedgerPostings` table) with `AccountId`, `Direction (Debit/Credit)`, `Amount`, `TransactionId` fields.
- **Cryptographic hash chain:** every new record stores `SHA256(PreviousRowHash + CurrentRowData)` — blockchain-style, which rules out a hidden amount edit even by a database administrator.
- **`UPDATE`/`DELETE` forbidden:** at the database level (Postgres `REVOKE UPDATE, DELETE` for the application role + triggers) and at the code level (`WriteAuditTrailActivity` already does only `INSERT` — we extend this behavior model to the whole ledger). An erroneous posting is reversed exclusively via a compensating (reversing) entry — this principle is already partially implemented in `ReleaseAccountBalanceActivity`/`ReleaseAccountBalanceConsumer` (compensation via a new record, not editing the old one) — we extend it to `CoreLedger.Service`.
- Tests: continuing the Testcontainers.Postgres pattern from the already-existing `IntegrationBus.CoreLedger.Service.Tests` — adding a check that the sum of debits strictly equals the sum of credits across any transaction's slice, and that the hash chain is never broken.

---

### Task 15. Anti-fraud and compliance: velocity checks + AML blacklists
**Dependencies:** task 14 (checks happen **before** a posting lands in the ledger), reuses Redis (already there for RedLock).

- Chain of Responsibility pattern: a new validator pipeline **before** `CheckComplianceLimitsActivity` (the existing RulesEngine engine in `Compliance.Service` remains the first level of limit checks; velocity/AML is a second, complementary level, not a replacement).
- *Velocity checks:* Redis counters (frequency/volume over a time window) — a natural extension of infrastructure already used by RedLock.
- *AML blacklists:* a table of blocked identifiers (IP/card/wallet/counterparty), with `FraudBlocked` as a new terminal saga state (analogous to the existing `Failed`).

---

### Task 16. FX Engine (core) — multi-currency accounts and spread
**Dependencies:** task 14 (double-entry — without it a multi-currency cross-payment physically cannot be posted correctly), task 15 (anti-fraud needs to see multi-currency operations too).

- Binding `Account` to ISO 4217 (we already have and use the `Currency` enum today — we extend it rather than reinventing it).
- **Important for correct sequencing:** at this stage `IExchangeRateProvider` is implemented as a **mock/deterministic provider** (a static rate table or a configurable fake in `appsettings`) — a real provider (Bank of Russia/exchanges) appears in Phase 3 (task 36) and is wired in via DI without changing the FX Engine's business logic. This is the standard "port/adapter" technique we already applied to `ITopicProducer<T>` in tests (`RecordingTopicProducer`/`FlakyTopicProducer`).
- Cross-currency processing: hold in the source currency → spread calculation → credit in the destination currency via double-entry (task 14) → the spread goes to the platform's revenue account.

---

### Task 17. Clearing and netting
**Dependencies:** task 14 (double-entry — the data source for aggregation), task 5 (Hangfire — the scheduled `ClearingJob`).

- Accumulation circuit: internal transactions between counterparties are posted instantly against the ledger, with no trip to an external bank.
- `ClearingJob` (a Hangfire recurring job) computes net balance (bilateral/multilateral netting) over a period and produces one aggregated transaction — this is the first **real consumer** of the future external banking gateway (Phase 3), even though the netting algorithm itself is fully self-contained.

---

### Task 18. Cost-Based Routing (core)
**Dependencies:** task 7 (Circuit Breaker — a required condition for Fallback Routing), task 16/17 (routing must know the currency and payment type).

- A `PaymentRoutes` table (fee, per channel) + a cost calculator for each available "pipe". At this stage, channels are **stubs** ("Bank A", "Bank B", "SBP mock") with configurable fees; real partners are wired in during Phase 3 (YuKassa, task 37) without changing the route-selection algorithm — exactly the same adapter pattern as task 16.
- Fallback through the already-ready Circuit Breaker (task 7): on a high error rate for a channel, automatic switching to the second-best option.

---

### Task 19. Compliance masking — extension + immutable audit trail
**Dependencies:** task 11 (Management.API — it's specifically its commands that need to be audited), task 2 (MediatR pipeline — the natural place for a decorator).

- We **already have** `SensitiveDataMaskingFilter<T>` (HMAC masking into Kafka security topics, already tested). The new task is not to duplicate this, but to **add a second circuit**: a `Scrutor` decorator over the `IRequestHandler` commands of `Management.API` (`services.Decorate<IRequestHandler<...>, AuditingDecorator<...>>()`), which writes a before/after diff into an immutable (append-only, following task 14) audit log tied to `UserId`/IP.
- The existing Kafka-masking filter continues to cover the bus message circuit; the new decorator covers the administrative action circuit — these are complementary mechanisms, not competing ones.

---

### Task 20. DLQ Inspector — backend and first frontend panel
**Dependencies:** task 5 (Hangfire retries), task 11 (Management.API), task 19 (every manual resend command must be audited).

- Backend: `GetDlqMessagesQuery`/`ResendDlqMessageCommand` (MediatR, task 2) on top of the table of undelivered messages that Hangfire (task 5) populates.
- Frontend: the first full feature panel, at `integration-bus-dashboard/features/dlq-inspector/` — a list + a Monaco Editor for editing the "raw" JSON + a "Resend" button.
- This is the first end-to-end vertical slice (DB → MediatR → Management.API → React panel) — it serves as the template for every subsequent panel (task 31).

---

### Task 21. Document engine: Fluid templates + Gotenberg
**Dependencies:** none external, but logically precedes EDM (Phase 3, task 38) and disputes (task 24).

- A `DocumentTemplates` table (`Code`, `HtmlContent` in Liquid/Fluid syntax, `Version`, `IsActive`).
- `Fluid.Core` renders HTML from a strictly typed C# DTO (no `string.Replace`).
- **PDF rendering via Gotenberg**, not `PuppeteerSharp`: Gotenberg runs as a separate Docker service (added to our single shared `docker-compose.yml`, like every other infrastructure container in this project), and doesn't leak memory inside the worker process. This is fundamentally **not an external SaaS integration** (no accounts/keys), just one more self-hosted container of our own — which is why the task belongs in Phase 2, not 3.

---

### Task 22. SkiaSharp — visual layer (QR codes/stamps/optimization)
**Dependencies:** task 21 (barcodes/stamps are embedded into the already-existing HTML→PDF pipeline).

- QR/DataMatrix codes — generated in memory, embedded into Fluid templates as Base64 before PDF assembly.
- A "digital signature" stamp (frame, certificate data) — for now we draw a **placeholder without a real signature** (the real signature — CryptoPro, Phase 3, task 39); the stamp as a graphic layer can and should be prepared in advance.
- Optimizing attachments (document scans) before uploading to S3 (task 23).

---

### Task 23. S3/MinIO — object storage
**Dependencies:** task 21/22 (there's something to store — PDFs and images).

> Formally this is an infrastructure component, but we place it here rather than in Phase 1, because introducing S3 before the first binary-file generator exists (task 21) has no justification — YAGNI. As soon as the first consumer appears (Fluid+Gotenberg+SkiaSharp), storage is wired in as the very next step.

- MinIO in `docker-compose.yml` for dev/CI, `AWSSDK.S3` in code — the "store by reference" pattern (the DB stores a URL + metadata, not a `byte[]`).
- Presigned URLs (10–15 min) for downloads through `Management.API` — one more reason Management.API (task 11) must exist earlier.

---

### Task 24. Notifications: SmartFormat + MimeKit/MailKit
**Dependencies:** task 5 (Hangfire — sending emails is offloaded to a background worker with retry), task 23 (attachments — PDF statements from S3).

- `SmartFormat.NET` for notification text templates (pluralization/branching with no C# code — edits go through DB templates, the same pattern as `DocumentTemplates` in task 21).
- `MimeKit`/`MailKit`: HTML+plain-text emails, attachments from S3. For dev/CI — a local SMTP container (MailHog/Papercut) in `docker-compose.yml`, so as not to depend on any external email provider at this stage (a real production SMTP relay is a separate infrastructure setup, not covered by this plan, since its specifics are not yet defined).

---

### Task 25. 2FA/TOTP for critical Management.API operations
**Dependencies:** task 11 (Management.API), task 19 (audit — TOTP protects exactly the actions being audited).

- `Otp.NET`, RFC 6238, fully local cryptography — **no external dependencies** (no SMS gateways), which puts this squarely in Phase 2.
- Applied to the critical operations already defined in task 19: manual DLQ resend (task 20), limit changes (RulesEngine JSON, already existing), audit-log export.

---

### Task 26. Multi-Auth (part 1): login/password + passwordless
**Dependencies:** task 24 (passwordless uses the email infrastructure), task 25 (2FA as a second factor on top of the password).

- `Users`/`UserIdentities` tables (the Identity User Links pattern — one user, multiple sign-in methods) — designed **with an eye toward future** OAuth/Keycloak providers (Phase 3, task 40), so the schema doesn't need redoing.
- Passwords — `BCrypt`/`Argon2id`, entirely in-house code, no external auth providers → Phase 2.
- Passwordless — a one-time code in Redis, sent through the already-ready email engine (task 24).

---

### Task 27. Billing and subscriptions (calculation engine)
**Dependencies:** task 9 (business metrics — the data source for billing metrics), task 6 (multi-tenancy — billing is inherently per-tenant).

- Pay-as-you-go / volume-based / subscription+overages — **calculation and invoice generation** are fully internal (using the already-collected `integration_bus_transactions_total`/`integration_bus_financial_volume` metrics). Actually accepting payment against the invoice is a separate external integration (YuKassa, Phase 3, task 37), so here we only build the calculation and invoice/statement-generation engine (through the already-ready Fluid+Gotenberg pipeline, task 21).

---

### Task 28. Dispute and chargeback management
**Dependencies:** task 14 (ledger — freezing via a compensating entry, not an `UPDATE`), task 23 (pulling documents from S3), task 11 (the "Disputes" UI section).

- A `Disputed` status — a new, compensable (not overwritable) transaction state, following the pattern of the already-existing `Failed` in `TransactionSagaStateMachine`.
- Automatic collection of supporting evidence from S3 (statements/receipts/invoices — available as tasks 21–23 and Phase 3 become ready).

---

### Task 29. HTML sanitization + international compliance skeleton
**Dependencies:** none hard, a logical close-out of Phase 2 before moving on to external integrations.

- `HtmlAgilityPack` for cleaning incoming HTML (product descriptions/content from source systems) of `<script>` before saving to the database — purely defensive coding, no external calls.
- **Skeleton only** for international compliance: configurable `ComplianceProfile` (RU/EU/US) as an enum/strategy + a GDPR "right to be forgotten" implementation (soft-delete with anonymization, without violating the ledger immutability from task 14 — referential PII is anonymized, not financial postings). Real external protocols (ISO 20022, Open Banking/PSD2) are out of scope for this plan — see the final note.

> Scenario A, `HtmlAgilityPack` for scraping bank-rate pages with no API, is deliberately pushed to Phase 3 (task 38) — that's a call out to an external, uncontrolled resource, not internal logic.

---

### Task 30. Frontend, wave 2 — remaining Management Dashboard panels
**Dependencies:** tasks 14–29 (each panel has a direct backend prototype introduced above).

Panels are wired in **in order of their backends' readiness**, not as one big block:
1. **Limit configurator** — on top of the RulesEngine JSON (already existing) + anti-fraud (task 15).
2. **Reconciliation Dashboard** — on top of the double-entry ledger (task 14).
3. **Data-mapping builder (React Flow)** — requires a separate backend metadata engine for system A/B field metadata; this is the only panel for which no ready backend prototype exists in the preceding tasks — it is designed and implemented at this step as a standalone mini-feature (schema metadata + a graph JSON + a backend interpreter).
4. **Routing Builder** — on top of Cost-Based Routing (task 18) and the future content-based routing (the same mechanics as the Routing Matrix).
5. **Billing Matrix** — on top of the billing engine (task 27).
6. **End-to-End Tracer** — search by `CorrelationId` on top of the already-ready and verified Jaeger/Loki tracing (task 9) — technically the cheapest panel, since all the heavy infrastructure is already working.

---

## 🌐 Phase 3. External Integrations

### Task 31. ATOL Online — cloud fiscalization (sandbox)
**Dependencies:** task 14 (immutable ledger — the source of `PaymentConfirmedEvent`), task 5 (Hangfire/Polly for polling receipt status), task 7 (Circuit Breaker on the ATOL HTTP client).

*Why first in Phase 3:* of all the external integrations, this is technically the simplest (no OAuth, no digital signature, no webhook infrastructure), and ATOL Online provides a publicly documented test sandbox — minimal risk, a quick first win in the external circuit.

- `FiscalizationWorker : BackgroundService`, listening for `PaymentConfirmedEvent` (from the already-existing saga/ledger — a real external payment through YuKassa, task 37, will plug into this same event later with no changes to the worker).
- Configuration in `appsettings.Development.json`: public test credentials from ATOL Online API v5's official sandbox documentation — with an explicit code comment that for demos to employers it's worth registering a personal sandbox, since the public test cash register shows other people's receipts.
- `POST /sell` → `uuid` → `GET /report/{uuid}` with a Polly retry (3–5 attempts, 2s interval) → recording the fiscal attributes (fiscal drive number, fiscal document sign) into the ledger.

---

### Task 32. External authorization providers (OAuth/OIDC) + Keycloak SSO
**Dependencies:** task 26 (the `Users`/`UserIdentities` schema is already designed for this), independent of the other external integrations — a good "parallel" quick win for Phase 3.

- Google/GitHub/VK ID/Yandex ID — standard ASP.NET Core OAuth packages, `ExternalId` links to `UserIdentities` (task 26).
- Keycloak (OIDC/SAML) — for large B2B clients, via `docker-compose.yml` (one more self-hosted container, but the authentication itself is a protocol/server external to our code — a different pattern from task 21/Gotenberg: here we're integrating with someone else's identity protocol, not standing up our own tool).

---

### Task 33. YuKassa — online payment acquiring
**Dependencies:** task 31 (a successful YuKassa payment triggers the same `PaymentConfirmedEvent` → the ATOL worker runs unchanged), task 7 (Circuit Breaker), task 15 (anti-fraud checks before `capture`).

- A two-step payment: `POST /payments` (`capture: false`) → webhook `/api/v1/payments/callback` (IP validated against a whitelist) → anti-fraud (task 15) → `POST /capture` → ledger (task 14) → fan-out to ATOL (task 31).
- An `Idempotence-Key` on every request — naturally layers on top of the already-ready `IdempotencyBehavior` (task 2).

---

### Task 34. DaData.NET — counterparty enrichment and validation (MDM)
**Dependencies:** none directly financial, but logically precedes EDM (task 36) — documents need to be sent to the counterparty's correct details.

- An enrichment step: using the tax ID/bank ID, pull the official name, tax registration code, state registration number, and status (active/being liquidated) — on "being liquidated", the transaction is rejected at the gate (one more compliance gate, complementing task 15).
- Fuzzy matching (Levenshtein distance / `SimMetrics.Net`) — deduplicating counterparties from different systems into a single golden record.

---

### Task 35. CryptoPro / cloud digital signature
**Dependencies:** task 22 (the digital-signature stamp graphic is already in place — here we wire in the real signature instead of the placeholder).

- A backend signing service — validating an incoming signature **before** initiating a transfer in the ledger (plugged in as one more step of the compliance pipeline, after task 15/34).
- Signing outgoing PDF statements (task 21) before sending them to EDM (task 36).

---

### Task 36. Generating closing documents + an EDM gateway (Diadoc/SBIS)
**Dependencies:** tasks 21–23 (templates/PDF/S3), 34 (correct counterparty details from DaData), 35 (signature).

The most complex integration in the plan — it pulls together almost the entire Phase 2 stack:
- A Hangfire recurring job (task 5) assembles monthly reconciliation-statement PDFs via Fluid+Gotenberg (task 21) from ledger data (task 14).
- `IEdmProvider` — an abstraction layer over Diadoc/SBIS (easy to add a second provider without changing the core).
- Upload to the operator's draft → digital signature (task 35) → send to the counterparty → a background worker listens for webhooks/polls statuses ("Delivered"/"Signed"/"Rejected") → syncs with the ledger.

---

### Task 37. Real exchange rates — replacing the FX Engine's mock provider
**Dependencies:** task 16 (the FX Engine is already designed around `IExchangeRateProvider` as a port).

- Primary path: the official Bank of Russia API / exchange APIs — a background worker (Hangfire, task 5) caches rates in Redis with a short TTL.
- Fallback for banks with no API: `HtmlAgilityPack` scraping of their public rate pages — it's precisely this call to an uncontrolled external resource (not the library itself) that makes this scenario an external integration, unlike task 29.
- Wired in via DI with not a single line of change to the `FxEngine` business logic (task 16) — exactly the benefit of the port/adapter architecture the mock was introduced for in the first place, back in Phase 2.

---

### Task 38. Full E2E coverage of external integrations + finalizing the QA circuit
**Dependencies:** all preceding external integrations (31–37) — there's nothing to test until they exist.

- Playwright scenarios now cover: a payment via YuKassa (sandbox) → ATOL fiscalization → appearing in the Reconciliation Dashboard; sending a statement via EDM → status change in the UI.
- The SonarCloud Quality Gate is tightened to its final target figures (≥80% coverage, 0 critical vulnerabilities), now across **all** of the code, including Phases 2–3.
- The Allure report becomes a full showcase of product maturity for demonstrating to stakeholders/employers.

---

### Task 39. International compliance and localization (strategic horizon)
**Dependencies:** task 29 (the compliance-profile skeleton already exists), task 36 (the EDM adapter pattern — the same technique for ISO 20022/Open Banking).

Deliberately placed at the end: this is not a concrete integration with a known sandbox (like ATOL/YuKassa/DaData), but a strategic direction — full support for ISO 20022 and Open Banking/PSD2 would require real banking partners and goes beyond a pet-project implementation. At this stage, the plan fixes an **extension point** (the architecture is already ready to accept a new `ComplianceProfile`/`IEdmProvider`-like adapter), but does not assume its immediate, full implementation.

---

## 📋 Final Combined Sequence

| # | Task | Phase | Key dependency |
|---|---|---|---|
| 1 | Markdown Link Checker (Lychee) | Infra | — |
| 2 | CQRS + MediatR | Infra | — |
| 3 | CloudEvents envelope | Infra | 2 |
| 4 | Mapperly | Infra | 2 |
| 5 | Hangfire | Infra | 2 |
| 6 | Multi-tenancy (schema) | Infra | — |
| 7 | Rate Limiting + Circuit Breaker | Infra | 6 |
| 8 | Resilience & HA (Patroni/Redis Cluster) | Infra | 6 |
| 9 | Deep Observability | Infra | 5, 8 |
| 10 | .NET Aspire | Infra | 9 |
| 11 | Management.API + SPA shell | Infra | 2, 7, 9 |
| 12 | DocFX | Infra | 3 |
| 13 | SonarCloud + Playwright + Allure | Infra | 11, 12 |
| 14 | Double-entry ledger + hash chain | Business | 6, 8 |
| 15 | Anti-fraud: velocity + AML | Business | 14 |
| 16 | FX Engine (core, mock rates) | Business | 14, 15 |
| 17 | Clearing and netting | Business | 5, 14 |
| 18 | Cost-Based Routing (core) | Business | 7, 16, 17 |
| 19 | Audit Trail + Scrutor decorators | Business | 2, 11 |
| 20 | DLQ Inspector (backend+panel) | Business | 5, 11, 19 |
| 21 | Fluid + Gotenberg (documents) | Business | — |
| 22 | SkiaSharp (QR codes/stamps) | Business | 21 |
| 23 | S3/MinIO | Business | 21, 22 |
| 24 | SmartFormat + MimeKit/MailKit | Business | 5, 23 |
| 25 | 2FA/TOTP | Business | 11, 19 |
| 26 | Multi-Auth: login/password + passwordless | Business | 24, 25 |
| 27 | Billing (calculation engine) | Business | 6, 9 |
| 28 | Disputes and chargebacks | Business | 14, 23 |
| 29 | HTML sanitization + international compliance skeleton | Business | — |
| 30 | Frontend, wave 2 (remaining panels) | Business | 14–29 |
| 31 | ATOL Online (sandbox) | External | 14, 5, 7 |
| 32 | OAuth/OIDC + Keycloak | External | 26 |
| 33 | YuKassa (acquiring) | External | 31, 7, 15 |
| 34 | DaData.NET (MDM) | External | — |
| 35 | CryptoPro/digital signature | External | 22 |
| 36 | Closing documents + EDM (Diadoc/SBIS) | External | 21–23, 34, 35 |
| 37 | Real exchange rates (Bank of Russia + scraping fallback) | External | 16 |
| 38 | Finalizing E2E/QA for external integrations | External | 31–37 |
| 39 | International compliance (strategic horizon) | External | 29, 36 |

---

## Notes on Scope

This is a maximally complete plan — **40 tasks**, covering the full evolution of the platform from its current state to a
fintech platform with external integrations. For actual execution, it makes sense to move in blocks (e.g. all of Phase 1
→ review → Phase 2 in blocks of 3-5 tasks → review → Phase 3), rather than trying to implement everything in one pass —
most of these tasks (especially 14, 21–24, 36) are comparable in scope to an entire production-readiness work stream
already completed on this repository (see [`docs/roadmap.md`](roadmap.md)).
