CREATE DATABASE IF NOT EXISTS analytics;

-- =====================================================================================
-- Balance domain: append-only journal entries (holds, releases, confirms, top-ups)
-- =====================================================================================

CREATE TABLE IF NOT EXISTS analytics.journal_entries_queue
(
    Id Int64,
    SourceAccountId UUID,
    TargetAccountId Nullable(UUID),
    SequenceNumber Int64,
    AmountDelta String,
    EntryType Int32,
    TransactionId UUID,
    TimestampUtc String,
    __deleted String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'integration-bus-kafka:9094',
    kafka_topic_list = 'cdc.balance.public.JournalEntries',
    kafka_group_name = 'clickhouse-journal-entries',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 5,
    kafka_handle_error_mode = 'stream';

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
    IngestedAtUtc DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (SourceAccountId, SequenceNumber);

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.journal_entries_mv
TO analytics.journal_entries
AS
SELECT
    Id,
    SourceAccountId,
    TargetAccountId,
    SequenceNumber,
    toDecimal64(AmountDelta, 4) AS AmountDelta,
    EntryType,
    TransactionId,
    parseDateTime64BestEffort(TimestampUtc, 6) AS TimestampUtc
FROM analytics.journal_entries_queue
WHERE __deleted = 'false';

-- =====================================================================================
-- Compliance domain: audit trail of every limit/rule verification outcome
-- =====================================================================================

CREATE TABLE IF NOT EXISTS analytics.compliance_audits_queue
(
    Id UUID,
    TransactionId UUID,
    SourceAccountId UUID,
    TargetAccountId UUID,
    Amount String,
    Currency Int32,
    Status Int32,
    FailureReason Nullable(String),
    CreatedAtUtc String,
    __deleted String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'integration-bus-kafka:9094',
    kafka_topic_list = 'cdc.compliance.public.ComplianceAudits',
    kafka_group_name = 'clickhouse-compliance-audits',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 5,
    kafka_handle_error_mode = 'stream';

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
    IngestedAtUtc DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (TransactionId, CreatedAtUtc);

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.compliance_audits_mv
TO analytics.compliance_audits
AS
SELECT
    Id,
    TransactionId,
    SourceAccountId,
    TargetAccountId,
    toDecimal64(Amount, 4) AS Amount,
    Currency,
    Status,
    FailureReason,
    parseDateTime64BestEffort(CreatedAtUtc, 6) AS CreatedAtUtc
FROM analytics.compliance_audits_queue
WHERE __deleted = 'false';

-- =====================================================================================
-- Ledger domain: immutable final audit trail records
-- =====================================================================================

CREATE TABLE IF NOT EXISTS analytics.ledger_entries_queue
(
    Id Int64,
    TransactionId UUID,
    Amount String,
    CreatedAt String,
    __deleted String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'integration-bus-kafka:9094',
    kafka_topic_list = 'cdc.ledger.public.LedgerEntries',
    kafka_group_name = 'clickhouse-ledger-entries',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 5,
    kafka_handle_error_mode = 'stream';

CREATE TABLE IF NOT EXISTS analytics.ledger_entries
(
    Id Int64,
    TransactionId UUID,
    Amount Decimal64(4),
    CreatedAt DateTime64(6),
    IngestedAtUtc DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (TransactionId, Id);

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.ledger_entries_mv
TO analytics.ledger_entries
AS
SELECT
    Id,
    TransactionId,
    toDecimal64(Amount, 4) AS Amount,
    parseDateTime64BestEffort(CreatedAt, 6) AS CreatedAt
FROM analytics.ledger_entries_queue
WHERE __deleted = 'false';

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
