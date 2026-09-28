using IntegrationBus.CoreLedger.Service.Models;
using MassTransit;
using StackExchange.Redis;

namespace IntegrationBus.CoreLedger.Service.Activities;

/// <summary>
/// Executes local high-performance transaction cache mutations and manages its stateless rollback compensation footprint.
/// </summary>
public sealed class UpdateCacheActivity(
    ILogger<UpdateCacheActivity> logger,
    IConnectionMultiplexer redis) : IActivity<UpdateCacheArguments, UpdateCacheLog>
{
    private static readonly TimeSpan CacheEntryTtl = TimeSpan.FromHours(1);

    private static string CacheKey(Guid transactionId) => $"ledger:tx:{transactionId}";

    /// <summary>
    /// Persists the committed transaction amount into the shared Redis cache so downstream reads avoid a database round-trip.
    /// </summary>
    public async Task<ExecutionResult> Execute(ExecuteContext<UpdateCacheArguments> context)
    {
        IDatabase database = redis.GetDatabase();

        await database.StringSetAsync(
            CacheKey(context.Arguments.TransactionId),
            context.Arguments.Amount.ToString(System.Globalization.CultureInfo.InvariantCulture),
            CacheEntryTtl);

        logger.LogInformation(
            "Courier Stage 2 | Cache: redis | SETEX ledger:tx:{TransactionId} {TtlSeconds} {Amount}",
            context.Arguments.TransactionId,
            CacheEntryTtl.TotalSeconds,
            context.Arguments.Amount);

        return context.Completed(new UpdateCacheLog
        {
            TransactionId = context.Arguments.TransactionId
        });
    }

    /// <summary>
    /// Evicts the cached transaction payload if a downstream step fails during the slip workflow execution.
    /// </summary>
    public async Task<CompensationResult> Compensate(CompensateContext<UpdateCacheLog> context)
    {
        IDatabase database = redis.GetDatabase();

        await database.KeyDeleteAsync(CacheKey(context.Log.TransactionId));

        logger.LogWarning(
            "Courier Compensation Triggered | Cache: redis | DEL ledger:tx:{TransactionId}",
            context.Log.TransactionId);

        return context.Compensated();
    }
}
