namespace IntegrationBus.Contracts;

/// <summary>
/// Centralized topology definitions mapping Kafka topic descriptors used across microservices.
/// </summary>
public static class KafkaTopics
{
    /// <summary>
    /// Topic utilized by the processing API gateway to initiate the stateful distributed saga orchestrator.
    /// </summary>
    public const string SagaTransactionStart = "saga-transaction-start";

    /// <summary>
    /// Inbound command topic used to trigger asynchronous balance replenishment.
    /// </summary>
    public const string AccountBalanceTopUp = "accounting-balance-topup";

    /// <summary>
    /// Event stream topic signaling successful execution of an individual account replenishment operation.
    /// </summary>
    public const string AccountBalanceTopUpPassed = "accounting-balance-topup-passed";

    /// <summary>
    /// Event stream topic signaling a processing or parsing failure during an account replenishment attempt.
    /// </summary>
    public const string AccountBalanceTopUpFailed = "accounting-balance-topup-failed";

    /// <summary>
    /// Inbound command orchestration stream topic utilized to trigger environment-gated database population utilities.
    /// </summary>
    public const string AccountDatabaseSeed = "account-database-seed";

    /// <summary>
    /// Event stream topic signaling that the massive test account generation sequence and database batch ingestion completed successfully.
    /// </summary>
    public const string AccountDatabaseSeedPassed = "account-database-seed-passed";

    /// <summary>
    /// Event stream topic signaling a processing, serialization, or database persistence boundary runtime failure during an account database seeding operation.
    /// </summary>
    public const string AccountDatabaseSeedFailed = "account-database-seed-failed";

    /// <summary>
    /// Inbound command topic mapping requests to execute a pessimistic asset reservation hold on an account balance.
    /// </summary>
    public const string AccountBalanceHold = "account-balance-hold";

    /// <summary>
    /// Event stream topic signaling that the required asset allocation has been successfully held in escrow.
    /// </summary>
    public const string AccountBalanceHoldPassed = "account-balance-hold-passed";

    /// <summary>
    /// Event stream topic indicating insufficient funds or missing account entities during a hold attempt.
    /// </summary>
    public const string AccountBalanceHoldFailed = "account-balance-hold-failed";

    /// <summary>
    /// Inbound command topic utilized to invoke compensatory transaction rules to unlock previously held assets.
    /// </summary>
    public const string AccountBalanceRelease = "account-balance-release";

    /// <summary>
    /// Specifies the Kafka messaging topic dedicated to streaming successful asset hold cancellation telemetry event payloads.
    /// </summary>
    public const string AccountBalanceReleasePassed = "account-balance-release-passed";

    /// <summary>
    /// Specifies the Kafka messaging topic dedicated to streaming infrastructure or data failure events encountered during compensation routines.
    /// </summary>
    public const string AccountBalanceReleaseFailed = "account-balance-release-failed";

    /// <summary>
    /// Specifies the Kafka messaging topic dedicated to streaming idempotency verification events indicating a compensation release step was bypassed safely.
    /// </summary>
    public const string AccountBalanceReleaseSkipped = "account-balance-release-skipped";

    /// <summary>
    /// Specifies the Kafka messaging topic dedicated to streaming explicit balance confirmation commands targeting the Accounting engine boundary.
    /// </summary>
    public const string AccountBalanceConfirm = "account-balance-confirm";

    /// <summary>
    /// Specifies the Kafka messaging topic dedicated to streaming successful double-entry ledger confirmation event telemetry payloads.
    /// </summary>
    public const string AccountBalanceConfirmPassed = "account-balance-confirm-passed";

    /// <summary>
    /// Specifies the Kafka messaging topic dedicated to streaming transaction confirmation breakdown or infrastructure failure event payloads.
    /// </summary>
    public const string AccountBalanceConfirmFailed = "account-balance-confirm-failed";

    /// <summary>
    /// Inbound command topic mapping anti-fraud scoring and velocity limit validation pipelines.
    /// </summary>
    public const string ComplianceLimitsCheck = "compliance-limits-check";

    /// <summary>
    /// Event stream topic confirming that the transaction parameters successfully cleared all compliance rules.
    /// </summary>
    public const string ComplianceLimitsCheckPassed = "compliance-limits-check-passed";

    /// <summary>
    /// Event stream topic signaling policy violations or blacklisting detection during compliance scoring.
    /// </summary>
    public const string ComplianceLimitsCheckFailed = "compliance-limits-check-failed";

    /// <summary>
    /// Inbound command topic designed to kickstart the multi-activity courier routing slip within the core ledger domain.
    /// </summary>
    public const string CoreLedgerRecordWrite = "core-ledger-record-write";

    /// <summary>
    /// Event stream topic documenting complete multi-engine operational commit of the financial journal entry.
    /// </summary>
    public const string CoreLedgerRecordWritePassed = "core-ledger-record-write-passed";

    /// <summary>
    /// Event stream topic capturing technical rollbacks or timeouts triggered inside the localized routing slip layer.
    /// </summary>
    public const string CoreLedgerRecordWriteFailed = "core-ledger-record-write-failed";
}
