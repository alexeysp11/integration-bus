namespace IntegrationBus.AccountBalance.Service.Entities;

/// <summary>
/// Represents a historical balance state calculation checkpoint utilized to short-circuit state reconstruction cycles.
/// </summary>
public sealed class AccountSnapshotEntity
{
    /// <summary>
    /// Gets or sets the unique primary key tracker index for the state checkpoint.
    /// </summary>
    public long Id { get; set; }

    /// <summary>
    /// Gets or sets the foreign key reference mapping back to the target core account identity.
    /// </summary>
    public Guid AccountId { get; set; }

    /// <summary>
    /// Gets or sets the exact ledger event sequence position up to which this state snapshot calculation is entirely inclusive.
    /// </summary>
    public long SequenceNumber { get; set; }

    /// <summary>
    /// Gets or sets the net accumulated financial balance captured precisely at the indexed sequence position boundary.
    /// </summary>
    public decimal SnapshotBalance { get; set; }

    /// <summary>
    /// Gets or sets the temporal marker tracking when this specific checkpoint state calculation was compiled and stored.
    /// </summary>
    public DateTime CapturedAtUtc { get; set; }
}
