using IntegrationBus.CoreLedger.Service.Models;
using MassTransit;

namespace IntegrationBus.CoreLedger.Service.Activities;

/// <summary>
/// Executes the terminal transactional notifications outbox dispatch sequence to finalized downstreams.
/// </summary>
public sealed class PublishLedgerCommittedActivity(ILogger<PublishLedgerCommittedActivity> logger)
    : IExecuteActivity<PublishLedgerCommittedArguments>
{
    public Task<ExecutionResult> Execute(ExecuteContext<PublishLedgerCommittedArguments> context)
    {
        logger.LogInformation(
            "Courier Stage 3 | Outbox | Emitting event: LedgerTransactionCommitted (TransactionId: {TransactionId}, Amount: {Amount})",
            context.Arguments.TransactionId,
            context.Arguments.Amount);

        return Task.FromResult(context.Completed());
    }
}
