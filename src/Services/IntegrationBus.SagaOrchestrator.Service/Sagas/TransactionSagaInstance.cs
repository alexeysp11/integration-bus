using MassTransit;

namespace IntegrationBus.SagaOrchestrator.Service.Sagas;

/// <summary>
/// Represents the persistent state database schema for the stateful transaction saga machine.
/// </summary>
public sealed class TransactionSagaInstance : SagaStateMachineInstance
{
    /// <summary>
    /// Gets or sets the unique tracking identifier for the saga instance. Maps to TransactionId.
    /// </summary>
    public Guid CorrelationId { get; set; }

    /// <summary>
    /// Gets or sets the current strongly-typed state of the saga machine mapped as an integer in the database index.
    /// </summary>
    public string CurrentState { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the source account identifier associated with the active transaction.
    /// </summary>
    public Guid SourceAccountId { get; set; }

    /// <summary>
    /// Gets or sets the target account identifier associated with the active transaction.
    /// </summary>
    public Guid TargetAccountId { get; set; }

    /// <summary>
    /// Gets or sets the financial volume being processed by the active transaction.
    /// </summary>
    public decimal Amount { get; set; }

    /// <summary>
    /// Gets or sets the currency type context for the active transaction.
    /// </summary>
    public int CurrencyId { get; set; }

    /// <summary>
    /// Gets or sets the timestamp indicating when the saga instance was originally created.
    /// </summary>
    public DateTime CreatedAt { get; set; }

    // TODO: add xml comment.
    public string? ErrorMessage {  get; set; }
}
