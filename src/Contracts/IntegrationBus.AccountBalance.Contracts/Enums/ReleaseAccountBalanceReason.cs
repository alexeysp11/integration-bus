namespace IntegrationBus.AccountBalance.Contracts.Enums;

/// <summary>
/// Specifies the deterministic root causes for triggering a balance compensation workflow.
/// </summary>
public enum ReleaseAccountBalanceReason
{
    /// <summary>
    /// Unspecified or invalid compensation reason placeholder.
    /// </summary>
    None = 0,

    /// <summary>
    /// The transaction breached regulatory, velocity, or AML limits managed by the Compliance service.
    /// </summary>
    ComplianceViolation = 1,

    /// <summary>
    /// The ledger infrastructure failed to record the final transaction record inside the Core Ledger service.
    /// </summary>
    LedgerWriteFailure = 2
}
