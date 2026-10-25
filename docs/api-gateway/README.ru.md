# 🛡️ Укрепление API Gateway и идентификация: NGINX Edge, Keycloak OIDC, gRPC-проверка токенов

[English](README.md) | [Русский](README.ru.md)

Каждый внешний запрос проходит три слоя, прежде чем попасть в бизнес-сервис:

```text
 Клиент ──HTTPS──▶ NGINX edge ──HTTP──▶ Gateway.Api (YARP) ──HTTP──▶ Processing.Api
                    │ TLS-терминация       │ проверка bearer-токена      (без кода аутентификации)
                    │ очистка заголовков   │        │ gRPC ValidateToken
                    │ rate limiting (429)  │        ▼
                                           Identity.Service ──OIDC discovery + JWKS──▶ Keycloak
```

Нижележащие сервисы не реализуют аутентификацию: запрос, дошедший до `Processing.Api`, уже аутентифицирован шлюзом.

---

## 1. NGINX Edge (TLS, очистка заголовков, rate limiting)

Конфигурация: [`infrastructure/nginx/nginx.conf`](../../infrastructure/nginx/nginx.conf) (Helm-чарт содержит
идентичную копию в `deploy/k8s/charts/integration-bus/files/nginx/nginx.conf`).

| Аспект | Поведение |
| :--- | :--- |
| TLS | Терминируется на порту `8443` (TLS 1.2/1.3, HSTS). Обычный HTTP на `8080` отдаёт только `/nginx-health`; любой другой запрос по HTTP получает `426 Upgrade Required`. |
| Сертификат | Docker Compose: одноразовый контейнер генерирует самоподписанный сертификат в том `integration-bus-nginx-certs`. Helm: `genSelfSignedCert` создаёт Secret `integration-bus-nginx-tls` и переиспользует его при обновлениях. Для реального имени хоста замените `tls.crt`/`tls.key` парой, подписанной CA. |
| Очистка заголовков | `X-Forwarded-*`, `X-Real-IP` и `Forwarded` перезаписываются значениями, увиденными на границе; `X-Authenticated-Subject` очищается; `Server` и `X-Powered-By` удаляются из ответов; некорректные заголовки отбрасываются. |
| Тело и таймауты | Тело запроса ограничено 1 МБ; таймауты заголовков, тела и отправки — 10–30 с. |
| Служебные эндпоинты | `/health` и `/metrics` через edge недоступны (`404`). |

### Лимиты запросов

Лимиты применяются на клиентский адрес через `limit_req`. Запрос сверх лимита получает
`429 Too Many Requests` с заголовком `Retry-After` **до проксирования** в `Gateway.Api`.

| Маршрут | Скорость | Burst |
| :--- | :--- | :--- |
| `POST /api/v1/ledger/transaction` | 10 запр/с | 20 |
| `/api/v1/accounts/**` | 10 запр/с | 20 |
| остальное | 50 запр/с | 100 |

Допускается не более 50 одновременных соединений на клиента. Значения настраиваются в директивах `limit_req_zone`
и `limit_req`. Если перед NGINX стоит другой прокси или балансировщик, ключом зон должен быть реальный адрес клиента
вместо `$binary_remote_addr`.

---

## 2. Keycloak (OAuth2 / OIDC)

Keycloak запускается в режиме разработки с realm из
[`infrastructure/keycloak/integration-bus-realm.json`](../../infrastructure/keycloak/integration-bus-realm.json).
Все перечисленные ниже учётные данные предназначены только для локальной разработки; замените их перед любым другим
использованием стенда.

| Элемент | Значение |
| :--- | :--- |
| Консоль администратора | `http://localhost:8180` (`admin` / `admin`) |
| Realm | `integration-bus` |
| Клиент | `integration-bus-client` (confidential, секрет `integration-bus-client-secret`, включены client-credentials и password grant) |
| Пользователь | `demo-user` / `demo-password` (роль realm `ledger-user`) |

Издатель (issuer) зафиксирован через `KC_HOSTNAME` (`http://localhost:8180` в Compose, `http://localhost:18180` для
Kind), поэтому токен, полученный с хоста, валиден внутри кластера, где сервис идентификации обращается к Keycloak по
внутреннему имени сервиса.

### Получение токена

```bash
TOKEN=$(curl -s -X POST http://localhost:8180/realms/integration-bus/protocol/openid-connect/token \
  -d grant_type=client_credentials \
  -d client_id=integration-bus-client \
  -d client_secret=integration-bus-client-secret | sed -E 's/.*"access_token":"([^"]+)".*/\1/')
```

---

## 3. gRPC-проверка токенов

Контракт: [`token_validation.proto`](../../src/Contracts/IntegrationBus.Identity.Contracts/Protos/token_validation.proto).
`Identity.Service` проверяет подпись (ключи из документа OIDC discovery, обновляются при ротации), издателя, срок
действия и, если задано, аудиторию. `Gateway.Api` вызывает его из `TokenValidationMiddleware` перед обратным прокси.

| Ситуация | Ответ шлюза |
| :--- | :--- |
| Нет заголовка `Authorization: Bearer` | `401 Unauthorized` + `WWW-Authenticate: Bearer` |
| Невалидный, просроченный или чужой токен | `401 Unauthorized` + `WWW-Authenticate: Bearer error="invalid_token"` |
| Сервис идентификации недоступен или не ответил вовремя | `503 Service Unavailable` (fail closed: непроверенный запрос никогда не проксируется) |
| `/health`, `/metrics`, `/openapi` | Пропускаются без токена |

После успешной проверки шлюз выставляет `X-Authenticated-Subject` равным субъекту токена (клиентское значение этого
заголовка предварительно удаляется). Роли realm возвращаются вызовом проверки для будущих правил авторизации.

### Ключи конфигурации

| Сервис | Ключ | Назначение |
| :--- | :--- | :--- |
| Gateway.Api | `Identity:Enabled` | Требовать токены (по умолчанию `true`); при `false` middleware и gRPC-клиент не регистрируются. |
| Gateway.Api | `Identity:GrpcAddress` | Адрес gRPC-эндпоинта сервиса идентификации (обязателен при включённой проверке, задаётся только конфигурацией). |
| Gateway.Api | `Identity:TimeoutSeconds` | Дедлайн одного вызова проверки. |
| Identity.Service | `Keycloak:Authority` | URL realm для OIDC discovery. |
| Identity.Service | `Keycloak:Audience` | Ожидаемая аудитория; пустое значение отключает проверку аудитории. |
| Identity.Service | `Keycloak:RequireHttpsMetadata` | Требовать HTTPS для discovery (отключено для локальной разработки). |

Identity.Service слушает `6005` (gRPC, HTTP/2 без TLS) и `6006` (`/health`, `/metrics`, HTTP/1.1).

---

## 4. Запуск и проверка

Точки входа Docker Compose: `https://localhost:8443` (edge), `http://localhost:5038` (шлюз напрямую, проверка токена
сохраняется), `http://localhost:8180` (Keycloak). Kind (Helm) использует те же порты плюс 10000, например `18443` и
`18180`.

```bash
# Без токена -> 401
curl -sk -o /dev/null -w "%{http_code}\n" -X POST https://localhost:8443/api/v1/ledger/transaction \
  -H "Content-Type: application/json" -d '{}'

# С валидным токеном -> запрос доходит до Processing.Api (400 для пустого тела, 202 для корректного)
curl -sk -o /dev/null -w "%{http_code}\n" -X POST https://localhost:8443/api/v1/ledger/transaction \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{}'

# Rate limit -> HTTP 429 после исчерпания burst-бюджета
for i in $(seq 1 60); do
  curl -sk -o /dev/null -w "%{http_code}\n" -X POST \
    https://localhost:8443/api/v1/accounts/00000000-0000-0000-0000-000000000001/topup \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{"amount":1,"currency":1}'
done | sort | uniq -c
```

Автоматические тесты: `tests/IntegrationBus.Gateway.Api.Tests` (поведение middleware) и
`tests/IntegrationBus.Identity.Service.Tests` (проверка токенов локально сгенерированными ключами подписи).

---

## 5. Диагностика

| Симптом | Причина и решение |
| :--- | :--- |
| `401` для только что выданного токена | Издатель токена отличается от издателя в документе discovery. Получайте токен по URL из `KC_HOSTNAME`, а не по другому имени хоста. |
| `503` от шлюза | Сервис идентификации не работает или не достаёт до Keycloak: проверьте `curl http://localhost:6006/health` и `docker compose logs integration-bus-identity-service`. |
| `429` при легитимной нагрузке | Увеличьте скорости зон в `nginx.conf` и перезапустите edge (`docker compose restart integration-bus-nginx`). |
| Предупреждение о сертификате в браузере или curl | Сертификат самоподписанный: передайте `-k` в curl или добавьте сертификат из тома `integration-bus-nginx-certs` в доверенные. |
