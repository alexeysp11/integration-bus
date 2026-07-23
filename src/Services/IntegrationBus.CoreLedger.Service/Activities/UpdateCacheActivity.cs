using IntegrationBus.CoreLedger.Service.Models;
using MassTransit;

namespace IntegrationBus.CoreLedger.Service.Activities;

/// <summary>
/// Executes local high-performance transaction cache mutations and manages its stateless rollback compensation footprint.
/// </summary>
public sealed class UpdateCacheActivity(ILogger<UpdateCacheActivity> logger) : IActivity<UpdateCacheArguments, UpdateCacheLog>
{
    /// <summary>
    /// Executes the fast-path memory cache synchronization simulation inside the local boundary.
    /// </summary>
    public async Task<ExecutionResult> Execute(ExecuteContext<UpdateCacheArguments> context)
    {
        logger.LogInformation(
            "Courier Stage 2 | Cache: redis | Executing command: SETEX ledger:tx:{TransactionId} 3600 {Amount}",
            context.Arguments.TransactionId,
            context.Arguments.Amount);

        return context.Completed(new UpdateCacheLog
        {
            TransactionId = context.Arguments.TransactionId
        });
    }

    /// <summary>
    /// Evicts or invalidates the cached transaction payload if a downstream step fails during the slip workflow execution.
    /// </summary>
    public Task<CompensationResult> Compensate(CompensateContext<UpdateCacheLog> context)
    {
        logger.LogWarning(
            "Courier Compensation Triggered | Cache: redis | Executing Eviction: DEL ledger:tx:{TransactionId}",
            context.Log.TransactionId);

        return Task.FromResult(context.Compensated());
    }
}
