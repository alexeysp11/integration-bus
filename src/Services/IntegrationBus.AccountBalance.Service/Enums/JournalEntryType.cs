namespace IntegrationBus.AccountBalance.Service.Enums;

/// <summary>
/// Specifies the deterministic operational intent and lifecycle state of a specific ledger journal entry.
/// </summary>
public enum JournalEntryType
{
    /// <summary>
    /// Represents a fallback value indicating an uninitialized or invalid ledger entry type state boundary.
    /// </summary>
    None = 0,

    /// <summary>
    /// Represents a temporary asset reservation withholding funds from active balance capacity.
    /// </summary>
    Hold = 1,

    /// <summary>
    /// Signals that a previously registered asset hold transaction finalized successfully and is now cleared.
    /// </summary>
    Confirmed = 2,

    /// <summary>
    /// Signals that a previously registered asset hold was aborted, releasing the reserved capacity back to the account.
    /// </summary>
    Cancelled = 3,

    /// <summary>
    /// Represents a direct, non-holding financial operation such as a balance top-up or an immediate credit deposition.
    /// </summary>
    DirectDeposit = 4
}
