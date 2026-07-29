using IntegrationBus.AccountBalance.Service.DbContexts;
using IntegrationBus.AccountBalance.Service.Entities;
using Microsoft.EntityFrameworkCore;

namespace IntegrationBus.AccountBalance.Service.Providers;

/// <summary>
/// Provides a high-performance state compilation engine that reconstructs financial balance entities through optimized ledger aggregations.
/// </summary>
public sealed class AccountStateReconstructor(BalanceDbContext dbContext) : IAccountStateReconstructor
{
    /// <inheritdoc />
    public async Task<decimal> ReconstructCurrentBalanceAsync(Guid accountId, CancellationToken cancellationToken)
    {
        // 1. Retrieve the latest available snapshot computation checkpoint for the target account record boundary
        AccountSnapshotEntity? latestSnapshot = await dbContext.Snapshots
            .Where(s => s.AccountId == accountId)
            .OrderByDescending(s => s.SequenceNumber)
            .FirstOrDefaultAsync(cancellationToken);

        long startingSequenceNumber = latestSnapshot?.SequenceNumber ?? 0;
        decimal baseBalance = latestSnapshot?.SnapshotBalance ?? 0.00m;

        // 2. Query and aggregate all subsequent append-only ledger transaction delta records generated past the snapshot checkpoint index
        decimal accumulatedDelta = await dbContext.JournalEntries
            .Where(j => j.SourceAccountId == accountId && j.SequenceNumber > startingSequenceNumber)
            .SumAsync(j => j.AmountDelta, cancellationToken);

        // 3. Compile the current exact operational balance value by merging the historical checkpoint base with streaming deltas
        return baseBalance + accumulatedDelta;
    }
}
