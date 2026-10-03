CREATE DATABASE IF NOT EXISTS analytics;

-- =====================================================================================
-- Ingestion note: rows are written by the official ClickHouse Kafka Connect Sink
-- connector (infrastructure/clickhouse-sink/*.json), not by ClickHouse's native Kafka
-- table engine. See docs/data-analytics/README.ru.md and docs/data-loading.ru.md for the
-- rationale: the Sink connector batches inserts and only commits its Kafka offset after
-- ClickHouse acknowledges the write (at-least-once), while ClickHouse's own insert block
-- deduplication (non_replicated_deduplication_window) -- combined with ReplacingMergeTree
-- on these single-node tables -- collapses any batch that gets retried/redelivered.
-- Each table also carries the raw `__deleted` flag Debezium's ExtractNewRecordState SMT
-- adds to every flattened CDC record, so hard deletes remain visible instead of silently
-- rejected by schema validation.
-- =====================================================================================

-- Balance domain: append-only journal entries (holds, releases, confirms, top-ups)
CREATE TABLE IF NOT EXISTS analytics.journal_entries
(
    Id Int64,
    SourceAccountId UUID,
    TargetAccountId Nullable(UUID),
    SequenceNumber Int64,
    AmountDelta Decimal64(4),
    EntryType Int32,
    TransactionId UUID,
    TimestampUtc DateTime64(6),
    __deleted String DEFAULT 'false',
    IngestedAtUtc DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree
ORDER BY (SourceAccountId, SequenceNumber)
SETTINGS non_replicated_deduplication_window = 100;

-- Compliance domain: audit trail of every limit/rule verification outcome
CREATE TABLE IF NOT EXISTS analytics.compliance_audits
(
    Id UUID,
    TransactionId UUID,
    SourceAccountId UUID,
    TargetAccountId UUID,
    Amount Decimal64(4),
    Currency Int32,
    Status Int32,
    FailureReason Nullable(String),
    CreatedAtUtc DateTime64(6),
    __deleted String DEFAULT 'false',
    IngestedAtUtc DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree
ORDER BY (TransactionId, Id)
SETTINGS non_replicated_deduplication_window = 100;

-- Ledger domain: immutable final audit trail records
CREATE TABLE IF NOT EXISTS analytics.ledger_entries
(
    Id Int64,
    TransactionId UUID,
    Amount Decimal64(4),
    CreatedAt DateTime64(6),
    __deleted String DEFAULT 'false',
    IngestedAtUtc DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree
ORDER BY (TransactionId, Id)
SETTINGS non_replicated_deduplication_window = 100;

-- =====================================================================================
-- Flat analytic cube joining the three CDC streams by TransactionId for reporting
-- =====================================================================================

CREATE VIEW IF NOT EXISTS analytics.transaction_cube AS
SELECT
    j.TransactionId AS TransactionId,
    j.SourceAccountId AS SourceAccountId,
    j.TargetAccountId AS TargetAccountId,
    j.EntryType AS JournalEntryType,
    j.AmountDelta AS JournalAmountDelta,
    c.Status AS ComplianceStatus,
    c.FailureReason AS ComplianceFailureReason,
    l.Amount AS LedgerAmount,
    l.CreatedAt AS LedgerCommittedAt
FROM analytics.journal_entries AS j
LEFT JOIN analytics.compliance_audits AS c ON c.TransactionId = j.TransactionId
LEFT JOIN analytics.ledger_entries AS l ON l.TransactionId = j.TransactionId
WHERE j.EntryType = 1; -- JournalEntryType.Hold: one row per originating transaction
