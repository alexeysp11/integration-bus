# Choosing a Data-Loading Architecture for ClickHouse

[English](data-loading.md) | [Русский](data-loading.ru.md)

This document records the alternatives considered for replicating changes from Postgres into ClickHouse, and why
the official **Kafka Connect ClickHouse Sink Connector** was chosen (see
[`docs/data-analytics/README.md`](README.md) for the current implementation).

---

## Comparison Criteria

* **Data consistency** — no row loss or duplication on worker/network failures.
* **OLTP load** — the loading process must not add extra load to the production Postgres database.
* **Delete and update support** — changes and `DELETE`s in the source must be visible in ClickHouse.
* **Amount of custom code** — a proven, industry-standard component is preferred over a custom worker, as long as
  it does not constrain the flexibility actually required.

---

## Alternatives Considered

### 1. Custom worker with incremental polling (`SELECT WHERE id > max_id`)

A periodic worker, every N minutes, selects rows from Postgres with an ID/date greater than the maximum value
already present in ClickHouse, and loads them in batches through a channel (`System.Threading.Channels`).

**Rejected**, because:
* **Hard deletes are invisible.** A row deleted in Postgres is never reflected in an incremental `SELECT` over an
  ascending ID — ClickHouse ends up with "zombie rows".
* **Updates to old rows are skipped.** An update to a row with an already-migrated (small) ID will not be picked
  up by the next selection without an additional index on `UpdatedAt`, which places a heavy indexing load on
  Postgres.
* **Periodic load on the OLTP database.** Every poll is a full analytical `SELECT` against the production
  database, as opposed to asynchronously reading the WAL log, which places zero load on the database engine.

### 2. Custom worker on top of the Debezium CDC stream

Instead of periodic polling — a worker that reads the already-prepared Debezium CDC stream from Kafka and loads
it into ClickHouse in batches via `System.Threading.Channels`.

**Rejected in favor of the ready-made connector**, because the industry standard for this task is the official
Kafka Connect Sink, not custom code: the connector already solves batching, offset management, and retries,
leaving no room for bugs in a home-grown reimplementation of the same protocol. For pipelines that need to
reconcile schemas across heterogeneous database engines (for example, differing data types or SQL dialects), a
custom worker with explicit field-by-field mapping remains a justified choice — but for the homogeneous
Postgres → ClickHouse pipeline in this project, there is no such need.

### 3. Official Kafka Connect ClickHouse Sink Connector (chosen)

The connector reads a batch of messages from a Kafka topic, does not commit the offset until the batch is
acknowledged by ClickHouse (`HTTP 200 OK`), and only then advances the offset. If a failure occurs at any step,
the connector re-reads the same offset after restart — an **at-least-once** guarantee. Re-sent rows are
automatically collapsed in ClickHouse via `ReplacingMergeTree` + `non_replicated_deduplication_window`, which
together yield eventual consistency with no row loss and no extra OLTP load — every comparison criterion above is
satisfied with no additional code.

Implementation details and the data schema are in [`docs/data-analytics/README.md`](README.md).
