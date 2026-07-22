using IntegrationBus.CoreLedger.Contracts.Messages.Events;
using IntegrationBus.CoreLedger.Service.Models;
using MassTransit;

namespace IntegrationBus.CoreLedger.Service.Activities;

/// <summary>
/// Executes the primary database transactional append log operations and manages its rollback compensation state.
/// </summary>
public sealed class WriteAuditTrailActivity(ILogger<WriteAuditTrailActivity> logger, ITopicProducer<WriteLedgerRecordFailed> failedProducer)
    : IActivity<WriteAuditTrailArguments, WriteAuditTrailLog>
{
    /// <summary>
    /// Executes the persistent transactional database record allocation simulation.
    /// </summary>
    public async Task<ExecutionResult> Execute(ExecuteContext<WriteAuditTrailArguments> context)
    {
        long generatedEntryId = DateTime.UtcNow.Ticks;

        try
        {
            logger.LogInformation(
                "Courier Stage 1 | Database: ledger | Executing SQL: INSERT INTO LedgerEntries (TransactionId, EntryId, Amount) VALUES ({TransactionId}, {EntryId}, {Amount})",
                context.Arguments.TransactionId,
                generatedEntryId,
                context.Arguments.Amount);

            // Complete step successfully and store execution metrics inside the stateless tracking log context
            return context.Completed(new WriteAuditTrailLog
            {
                TransactionId = context.Arguments.TransactionId,
                LedgerEntryId = generatedEntryId
            });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Fatal crash inside WriteAuditTrail.Execute. Executing explicit internal self-compensation.");
            return context.Faulted(ex);
        }
    }

    /// <summary>
    /// Compensates the primary transaction entry if a downstream activity within the routing slip lifecycle fails.
    /// </summary>
    public async Task<CompensationResult> Compensate(CompensateContext<WriteAuditTrailLog> context)
    {
        logger.LogWarning(
            "Courier Compensation Triggered | Database: ledger | Executing Rollback SQL: UPDATE LedgerEntries SET Status = 'Compensated' WHERE EntryId = {EntryId} AND TransactionId = {TransactionId}",
            context.Log.LedgerEntryId,
            context.Log.TransactionId);

        return context.Compensated();
    }
}
