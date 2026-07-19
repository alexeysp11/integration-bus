namespace IntegrationBus.Contracts.Enums;

/// <summary>
/// Specifies the lifecycle execution states of a distributed transaction.
/// </summary>
public enum TransactionStatus
{
    /// <summary>
    /// Unspecified or invalid transaction status placeholder.
    /// </summary>
    None = 0,

    /// <summary>
    /// The transaction has been accepted and is currently being processed by the saga.
    /// </summary>
    Processing = 1,

    /// <summary>
    /// All distributed steps completed successfully; the transaction is finalized.
    /// </summary>
    Completed = 2,

    /// <summary>
    /// The transaction breached compliance, validation, or infrastructure rules and was rolled back.
    /// </summary>
    Failed = 3
}
