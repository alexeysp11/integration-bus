# 🛡️ API Gateway Hardening & Identity: NGINX Edge, Keycloak OIDC, gRPC Token Validation

[English](README.md) | [Русский](README.ru.md)

Every external request passes three layers before it reaches a business service:

```text
 Client ──HTTPS──▶ NGINX edge ──HTTP──▶ Gateway.Api (YARP) ──HTTP──▶ Processing.Api
                    │ TLS termination      │ bearer token check          (no authentication code)
                    │ header sanitizing    │        │ gRPC ValidateToken
                    │ rate limiting (429)  │        ▼
                                           Identity.Service ──OIDC discovery + JWKS──▶ Keycloak
```

Downstream services never implement authentication: a request that reaches `Processing.Api` has already been
authenticated by the gateway.

---

## 1. NGINX Edge (TLS, Header Sanitization, Rate Limiting)

Configuration: [`infrastructure/nginx/nginx.conf`](../../infrastructure/nginx/nginx.conf) (the Helm chart ships an
identical copy in `deploy/k8s/charts/integration-bus/files/nginx/nginx.conf`).

| Concern | Behavior |
| :--- | :--- |
| TLS | Terminated on port `8443` (TLS 1.2/1.3, HSTS). Plain HTTP on `8080` serves only `/nginx-health`; every other plain request receives `426 Upgrade Required`. |
| Certificate | Docker Compose: a one-shot container generates a self-signed certificate into the `integration-bus-nginx-certs` volume. Helm: `genSelfSignedCert` creates the Secret `integration-bus-nginx-tls` and reuses it on upgrades. Replace `tls.crt`/`tls.key` with a CA-signed pair for a real hostname. |
| Header sanitization | `X-Forwarded-*`, `X-Real-IP` and `Forwarded` are overwritten with edge-observed values, `X-Authenticated-Subject` is cleared, `Server`/`X-Powered-By` are removed from responses, invalid headers are dropped. |
| Body and timeouts | Request body limited to 1 MB; header, body and send timeouts of 10–30 s. |
| Operational endpoints | `/health` and `/metrics` are not exposed through the edge (`404`). |

### Rate limits

Limits are enforced per client address with `limit_req`. A request over budget receives
`429 Too Many Requests` with a `Retry-After` header **before it is proxied** to `Gateway.Api`.

| Route | Rate | Burst |
| :--- | :--- | :--- |
| `POST /api/v1/ledger/transaction` | 10 r/s | 20 |
| `/api/v1/accounts/**` | 10 r/s | 20 |
| everything else | 50 r/s | 100 |

At most 50 concurrent connections per client are allowed. Tune the numbers in the `limit_req_zone` and `limit_req`
directives. Behind another proxy or load balancer, key the zones on the real client address instead of
`$binary_remote_addr`.

---

## 2. Keycloak (OAuth2 / OIDC)

Keycloak runs in development mode with the realm imported from
[`infrastructure/keycloak/integration-bus-realm.json`](../../infrastructure/keycloak/integration-bus-realm.json).
All credentials below exist for local development only; replace them before exposing the stack anywhere else.

| Item | Value |
| :--- | :--- |
| Admin console | `http://localhost:8180` (`admin` / `admin`) |
| Realm | `integration-bus` |
| Client | `integration-bus-client` (confidential, secret `integration-bus-client-secret`, client-credentials and password grants enabled) |
| User | `demo-user` / `demo-password` (realm role `ledger-user`) |

The issuer is pinned with `KC_HOSTNAME` (`http://localhost:8180` in Compose, `http://localhost:18180` for Kind) so a
token obtained from the host validates inside the cluster, where the identity service reaches Keycloak by its internal
service name.

### Obtaining a token

```bash
TOKEN=$(curl -s -X POST http://localhost:8180/realms/integration-bus/protocol/openid-connect/token \
  -d grant_type=client_credentials \
  -d client_id=integration-bus-client \
  -d client_secret=integration-bus-client-secret | sed -E 's/.*"access_token":"([^"]+)".*/\1/')
```

---

## 3. gRPC Token Validation

Contract: [`token_validation.proto`](../../src/Contracts/IntegrationBus.Identity.Contracts/Protos/token_validation.proto).
`Identity.Service` validates the signature (keys from the OIDC discovery document, refreshed on rotation), the issuer,
the lifetime and, when configured, the audience. `Gateway.Api` calls it through `TokenValidationMiddleware` before the
reverse proxy.

| Situation | Gateway response |
| :--- | :--- |
| No `Authorization: Bearer` header | `401 Unauthorized` + `WWW-Authenticate: Bearer` |
| Invalid, expired or foreign token | `401 Unauthorized` + `WWW-Authenticate: Bearer error="invalid_token"` |
| Identity service unreachable or timed out | `503 Service Unavailable` (fail closed: an unverified request is never proxied) |
| `/health`, `/metrics`, `/openapi` | Passed without a token |

After a successful check the gateway sets `X-Authenticated-Subject` to the token subject (any client-supplied value is
removed first). Realm roles are returned by the validation call for future authorization rules.

### Configuration keys

| Service | Key | Meaning |
| :--- | :--- | :--- |
| Gateway.Api | `Identity:Enabled` | Enforce tokens (default `true`); when `false` the middleware and the gRPC client are not registered. |
| Gateway.Api | `Identity:GrpcAddress` | Address of the identity service gRPC endpoint (required when enabled, supplied through configuration only). |
| Gateway.Api | `Identity:TimeoutSeconds` | Deadline of one validation call. |
| Identity.Service | `Keycloak:Authority` | Realm URL used for OIDC discovery. |
| Identity.Service | `Keycloak:Audience` | Expected audience; empty disables the audience check. |
| Identity.Service | `Keycloak:RequireHttpsMetadata` | Require HTTPS for discovery (disabled for local development). |

Identity.Service listens on `6005` (gRPC, HTTP/2 without TLS) and `6006` (`/health`, `/metrics`, HTTP/1.1).

---

## 4. Running and Verifying

Docker Compose entry points: `https://localhost:8443` (edge), `http://localhost:5038` (gateway, direct, still
authenticated), `http://localhost:8180` (Keycloak). Kind (Helm) uses the same ports plus 10000, for example `18443`
and `18180`.

```bash
# No token -> 401
curl -sk -o /dev/null -w "%{http_code}\n" -X POST https://localhost:8443/api/v1/ledger/transaction \
  -H "Content-Type: application/json" -d '{}'

# Valid token -> the request reaches Processing.Api (400 for an empty body, 202 for a valid one)
curl -sk -o /dev/null -w "%{http_code}\n" -X POST https://localhost:8443/api/v1/ledger/transaction \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{}'

# Rate limit -> HTTP 429 once the burst budget is exhausted
for i in $(seq 1 60); do
  curl -sk -o /dev/null -w "%{http_code}\n" -X POST \
    https://localhost:8443/api/v1/accounts/00000000-0000-0000-0000-000000000001/topup \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{"amount":1,"currency":1}'
done | sort | uniq -c
```

Automated coverage: `tests/IntegrationBus.Gateway.Api.Tests` (middleware behavior) and
`tests/IntegrationBus.Identity.Service.Tests` (token validation with locally generated signing keys).

---

## 5. Troubleshooting

| Symptom | Cause and fix |
| :--- | :--- |
| `401` for a token that was just issued | The token issuer differs from the issuer in the discovery document. Obtain the token through the URL configured in `KC_HOSTNAME`, not through another host name. |
| `503` from the gateway | The identity service is down or cannot reach Keycloak: check `curl http://localhost:6006/health` and `docker compose logs integration-bus-identity-service`. |
| `429` during legitimate load | Raise the zone rates in `nginx.conf` and restart the edge (`docker compose restart integration-bus-nginx`). |
| Certificate warning in a browser or curl | The certificate is self-signed; pass `-k` to curl or trust the certificate from the `integration-bus-nginx-certs` volume. |
