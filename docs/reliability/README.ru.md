# 🛡️ Reliability Engineering: Distributed Locks & Rules Engine

[English](README.md) | [Русский](README.ru.md)

Описывает архитектуру распределённых блокировок (Redis / RedLock.net) вокруг критической секции баланса и структуру
декларативного JSON-движка правил комплаенса (`RulesEngine`), появившихся в `IntegrationBus.AccountBalance.Service`
и `IntegrationBus.Compliance.Service`.

---

## 1. Проблема, которую решают распределённые блокировки

До этого изменения `HoldAccountBalanceConsumer` читал текущий доступный баланс счёта (через
`IAccountStateReconstructor.ReconstructAvailableBalanceAsync`) и затем — **в той же самой** транзакции БД — вставлял
hold-запись в журнал. Так как Npgsql по умолчанию использует уровень изоляции `Read Committed`, а сама sequence-based
модель event sourcing не блокирует чтение баланса чем-то вроде `SELECT ... FOR UPDATE`, при **двух репликах** сервиса
`account-balance-service`, обрабатывающих `HoldAccountBalance` для **одного и того же** счёта одновременно (Kafka не
гарантирует, что оба сообщения попадут на одну и ту же партицию/реплику, так как партиционирование по `AccountId`
пока не реализовано — см. [`docs/roadmap.md`](../roadmap.md), Stage 6), возможен классический TOCTOU (time-of-check-to-time-of-use)
race:

1. Реплика A читает баланс счёта: 100.
2. Реплика B читает баланс того же счёта: 100 (до того как A успела закоммитить).
3. A резервирует 80 — проверка `100 >= 80` проходит.
4. B резервирует 80 — проверка `100 >= 80` тоже проходит (B ещё не видела изменений A).
5. Итог: с одного счёта на 100 списано 160 — овердрафт.

Уникальный индекс `(SourceAccountId, SequenceNumber)` от этого не защищает, так как обе реплики вставляют записи с
**разными** `SequenceNumber` — конфликта уникальности не возникает.

---

## 2. Решение: Redis Distributed Lock (RedLock.net)

### 2.1 Где подключено
- Пакеты: `RedLock.net` + `StackExchange.Redis` (`IntegrationBus.AccountBalance.Service.csproj`).
- Регистрация DI (`Program.cs`):
  ```csharp
  builder.Services.AddSingleton<IConnectionMultiplexer>(
      _ => ConnectionMultiplexer.Connect(redisConnectionString));

  builder.Services.AddSingleton<IDistributedLockFactory>(sp =>
      RedLockFactory.Create([new RedLockMultiplexer(sp.GetRequiredService<IConnectionMultiplexer>())]));
  ```
- Использование (`Consumers/HoldAccountBalanceConsumer.cs`): перед открытием DB-транзакции консьюмер берёт лок по
  ключу `account-lock:{AccountFromId}`:
  ```csharp
  string lockResource = $"account-lock:{message.AccountFromId}";
  using IRedLock accountLock = await lockFactory.CreateLockAsync(
      lockResource, expiryTime: 15s, waitTime: 10s, retryTime: 200ms, cancellationToken);

  if (!accountLock.IsAcquired)
  {
      // публикуем HoldAccountBalanceFailed с явной причиной — fail-safe, а не зависание/тихий сбой
      return;
  }
  // ... вся критическая секция (чтение баланса + вставка hold-записи) выполняется под локом
  ```

### 2.2 Параметры лока и почему именно такие
| Параметр | Значение | Обоснование |
|---|---|---|
| `expiryTime` | 15 секунд | TTL лока в Redis. Если процесс-держатель лока упадёт (краш пода) до явного освобождения, лок автоматически истечёт и не заблокирует счёт навечно. |
| `waitTime` | 10 секунд | Сколько ждущий поток пытается получить лок, прежде чем сдаться. |
| `retryTime` | 200 мс | Интервал между попытками захвата в течение `waitTime`. |

### 2.3 Сценарии дедлоков и почему их здесь нет
Классический распределённый дедлок возникает, когда поток A держит лок на ресурсе `X` и ждёт лок на `Y`, а поток B
одновременно держит лок на `Y` и ждёт `X`. В текущей реализации `HoldAccountBalanceConsumer` берёт **ровно один**
лок за раз (`account-lock:{AccountFromId}`) и никогда не пытается захватить второй лок, находясь под первым — поэтому
цикличное ожидание (условие возникновения дедлока) структурно невозможно для этого пути.

**Важное ограничение (зафиксировано намеренно, не оставлено случайно):** блокируется только `AccountFromId` (счёт,
с которого списываются средства), потому что именно на этом счету изменяется доступный баланс на шаге `Hold`.
`ConfirmAccountBalanceConsumer`/`ReleaseAccountBalanceConsumer` в текущей версии не берут Redis-лок — они защищены
от дублей отдельным механизмом идемпотентности (проверка "уже существует запись с таким `TransactionId`" перед
вставкой, см. п. 3), а не блокировкой счёта, так как их работа опирается на уже созданную hold-запись, найденную по
`TransactionId`, а не на пересчёт баланса с нуля.

### 2.4 Что произойдёт при недоступности Redis
`CreateLockAsync` в этом случае не бросает исключение, а возвращает лок с `IsAcquired == false`. Консьюмер
интерпретирует это как явный, ожидаемый сбой (fail-safe) и публикует `HoldAccountBalanceFailed` с понятной причиной —
вместо зависания или, что хуже, продолжения работы без защиты от гонки (fail-open). `AspNetCore.HealthChecks.Redis`
на `GET /health` заранее сигнализирует об этой деградации до того, как она начнёт приводить к отказам транзакций.

### 2.5 Осознанное упрощение: один Redis-инстанс, а не полноценный Redlock-кворум
Алгоритм Redlock в его оригинальной формулировке рассчитан на **N ≥ 3 независимых** Redis-мастеров и требует кворума
большинства для гарантии безопасности при сетевых партициях. В `docker-compose.yml` поднят **один** инстанс
(`integration-bus-redis`, с `--appendonly yes` для персистентности). Для целей этого проекта это осознанный
компромисс: используется тот же клиент `RedLock.net`, но по факту это простая взаимная блокировка поверх одного
Redis, чего достаточно, чтобы устранить продемонстрированную гонку в staging/single-node окружении. Для боевого
продакшена с более высокими требованиями к отказоустойчивости следующим шагом было бы поднять 3–5 независимых Redis
инстансов и передать все их адреса в `RedLockFactory.Create([...])`.

---

## 3. Идемпотентность (закрыта попутно, так как напрямую связана с защитой от гонок)

При аудите обнаружилось, что `HoldAccountBalanceConsumer`, `ConfirmAccountBalanceConsumer`,
`ReleaseAccountBalanceConsumer` и `TopUpAccountBalanceConsumer` не проверяли, не был ли `TransactionId` уже обработан
ранее — при повторной доставке сообщения Kafka (at-least-once delivery) каждый из них создавал **вторую**
журнальную запись, задваивая списание/зачисление. Во все четыре консьюмера добавлена проверка "запись с таким
`TransactionId` и таким `EntryType` уже существует" **перед** вставкой; при обнаружении дубликата консьюмер
коммитит пустую транзакцию и повторно публикует событие `...Passed` (идемпотентный replay), не изменяя баланс
повторно.

---

## 4. Compliance Rules Engine: декларативные JSON-правила

### 4.1 Было
`CheckComplianceLimitsConsumer` безусловно писал в БД аудит-запись со статусом `Passed` и публиковал
`CheckComplianceLimitsPassed` — никакой реальной проверки лимитов не существовало.

### 4.2 Стало
Пакет `RulesEngine` (Microsoft, NuGet ID `RulesEngine`, v6.0.1) загружает workflow из
`src/Services/IntegrationBus.Compliance.Service/Rules/compliance-rules.json` один раз при старте сервиса
(`ComplianceRulesEvaluator`, зарегистрирован как singleton), и на каждый `CheckComplianceLimits` выполняет
`ExecuteAllRulesAsync` над плоской проекцией сообщения (`ComplianceRuleInput`). Первое нарушенное правило становится
причиной отказа (`ComplianceEvaluationResult.FailureReason`), которая записывается в `ComplianceAudits.FailureReason`
и уходит в `CheckComplianceLimitsFailed.Reason`.

### 4.3 Структура JSON-файла правил
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
- **`WorkflowName`** — имя workflow, которое передаётся в `ExecuteAllRulesAsync(workflowName, ...)`; должно совпадать
  с константой `WorkflowName` в `ComplianceRulesEvaluator.cs`.
- **`Rules[].RuleName`** — уникальное имя правила, попадает в лог и в `FailureReason` (как запасной вариант, если
  `ErrorMessage` не задан).
- **`Rules[].Expression`** — C#-подобное лямбда-выражение, скомпилированное `RulesEngine` в реальный делегат;
  параметр `input` — это экземпляр `ComplianceRuleInput`, переданный как `new RuleParameter("input", ruleInput)`.
- **`Rules[].ErrorMessage`** — человекочитаемая причина отказа, возвращается наружу как есть.
- **`Rules[].RuleExpressionType`** — `LambdaExpression` для всех правил в этом файле.

Сейчас настроено 4 правила: положительная сумма, максимум одной транзакции (1,000,000), различие счетов
отправителя/получателя, допустимый диапазон кода валюты (`Currency` enum, 1–11).

### 4.4 Как добавить новое правило
1. Добавить объект в массив `Rules` в `compliance-rules.json` (файл копируется в выходную директорию сборки —
   `CopyToOutputDirectory: PreserveNewest`, путь настраивается через `ComplianceRules:RulesFilePath` в
   `appsettings.json`, по умолчанию `Rules/compliance-rules.json`).
2. Если правилу нужно новое поле сообщения — добавить его в `ComplianceRuleInput` и в маппинг внутри
   `CheckComplianceLimitsConsumer.Consume`.
3. **Известное ограничение:** правила загружаются один раз при старте сервиса (singleton), горячей перезагрузки
   файла во время работы нет — после правки `compliance-rules.json` сервис нужно перезапустить
   (`docker compose restart integration-bus-compliance-service`).

### 4.5 Тесты
`tests/IntegrationBus.Compliance.Service.Tests/Rules/ComplianceRulesEvaluatorTests.cs` подключает **тот самый**
production-файл правил (через MSBuild `<None Include="..." Link="...">`, а не копию), поэтому регрессия в самом
JSON-файле будет обнаружена тестами, а не только в рантайме.
