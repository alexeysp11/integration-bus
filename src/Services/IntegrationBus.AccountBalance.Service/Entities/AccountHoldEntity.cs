namespace IntegrationBus.AccountBalance.Service.Entities;

/// <summary>
/// Represents the persistent transaction balance freezing record used to secure a specific amount of funds inside isolated distributed sagas.
/// </summary>
public sealed class AccountHoldEntity
{
    /// <summary>
    /// Gets or sets the auto incremented database transactional row primary identifier key tracking index sequence boundary.
    /// </summary>
    public long Id { get; set; }

    /// <summary>
    /// Gets or sets the idempotent unique identity tracking identifier mapping back to the initial orchestration transaction request framework.
    /// </summary>
    public Guid TransactionId { get; set; }

    /// <summary>
    /// Gets or sets the reference identifier linking the transaction hold to a specific target balance account entity holder record.
    /// </summary>
    public Guid AccountId { get; set; }

    /// <summary>
    /// Gets or sets the specific volume of financial assets locked and withheld from being withdrawn until transaction resolution finalizes.
    /// </summary>
    public decimal Amount { get; set; }

    /// <summary>
    /// Gets or sets the temporal timestamp tracking exactly when the specific balance restriction record was initially registered.
    /// </summary>
    public DateTime CreatedAt { get; set; }
}
