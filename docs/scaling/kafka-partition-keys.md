# Kafka Partition Key Routing

[English](kafka-partition-keys.md) | [Русский](kafka-partition-keys.ru.md)

Every MassTransit Kafka Rider producer in the platform stamps each outbound record with an explicit partition key.
The key is registered through `AddPartitionKeyedProducer<T>` in
[`KafkaPartitionKeyExtensions.cs`](../../src/Libraries/IntegrationBus.Shared/Extensions/KafkaPartitionKeyExtensions.cs).

## Why keys are required

Kafka assigns a record to a partition by hashing its key. All records that share a key are appended to the same
partition, and a partition is consumed by exactly one member of a consumer group at a time. Routing by key therefore
provides two guarantees:

* All operations against one financial account are processed by a single service replica, in the order they were
  produced.
* All saga events of one transaction reach the same orchestrator replica in the order they were produced.

Without a key the producer spreads records over partitions (sticky round-robin), which lets two replicas process
messages of the same account concurrently and out of order.

## Key selection

| Message family | Key | Reason |
| :--- | :--- | :--- |
| `StartTransactionSaga`, saga outcome events, `CheckComplianceLimits`, `WriteLedgerRecord`, `ConfirmAccountBalance` | `TransactionId` | Keeps one saga's commands and events ordered on one partition. |
| `HoldAccountBalance` | `AccountFromId` | Serializes holds against the same source account. |
| `ReleaseAccountBalance`, `TopUpAccountBalance` and its outcome events | `AccountId` | Serializes balance mutations of one account. |
| `SeedAccountDatabaseBulkData` and its outcome events | none | A bulk command is not tied to a single account or transaction. |

The key is the lowercase hyphenated string form of the identifier (`Guid.ToString("D")`).

## Verification

`KafkaPartitionKeyRoutingTests` in `tests/IntegrationBus.Shared.Tests` runs against a real Kafka broker and asserts that:

* every record carries the account identifier as its key and each account maps to exactly one partition;
* with two consumers in one consumer group, each account is processed by exactly one of them, in production order.
