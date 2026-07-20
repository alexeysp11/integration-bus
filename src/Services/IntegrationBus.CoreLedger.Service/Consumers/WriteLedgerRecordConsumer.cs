using MassTransit;
using IntegrationBus.CoreLedger.Contracts.Messages.Commands;
using IntegrationBus.CoreLedger.Contracts.Messages.Events;

namespace IntegrationBus.CoreLedger.Service.Consumers;

/// <summary>
/// Handles incoming immutable transaction logging requests from the global Saga Orchestrator.
/// </summary>
public sealed class WriteLedgerRecordConsumer(ILogger<WriteLedgerRecordConsumer> logger) : IConsumer<WriteLedgerRecord>
{
    /// <summary>
    /// Executes the immutable core ledger transaction commitment simulation.
    /// </summary>
    public async Task Consume(ConsumeContext<WriteLedgerRecord> context)
    {
        // Audit log simulating foundational database write execution flow
        logger.LogInformation(
            "Database: ledger | Executing SQL: INSERT INTO LedgerEntries (TransactionId, SourceAccountId, TargetAccountId, Amount, Currency) VALUES ({TransactionId}, {SourceAccount}, {TargetAccount}, {Amount}, {Currency})",
            context.Message.TransactionId,
            context.Message.SourceAccountId,
            context.Message.TargetAccountId,
            context.Message.Amount,
            context.Message.Currency);

        // Generate synthetic sequenced primary key entry identifier required by the event schema
        long technicalEntryId = DateTime.UtcNow.Ticks;

        // Respond back to the Saga Orchestrator over Kafka mirroring the outcome pattern
        await context.RespondAsync(new WriteLedgerRecordPassed
        {
            TransactionId = context.Message.TransactionId,
            EntryId = technicalEntryId,
            CreatedAt = DateTime.UtcNow
        });
    }
}
