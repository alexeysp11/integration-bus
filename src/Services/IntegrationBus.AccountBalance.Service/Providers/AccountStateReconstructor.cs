using System.Data.Common;
using Dapper;
using IntegrationBus.AccountBalance.Service.DbContexts;
using IntegrationBus.AccountBalance.Service.Entities;

namespace IntegrationBus.AccountBalance.Service.Providers;

/// <summary>
/// Reconstructs the actual state ledger balances by applying immutable event log sequences onto checkpoint snapshots.
/// </summary>
public sealed class AccountStateReconstructor : IAccountStateReconstructor
{
    private const string SnapshotSql = $@"
        SELECT ""{nameof(AccountSnapshotEntity.SequenceNumber)}"", ""{nameof(AccountSnapshotEntity.SnapshotBalance)}""
        FROM ""{nameof(BalanceDbContext.Snapshots)}""
        WHERE ""{nameof(AccountSnapshotEntity.AccountId)}"" = @AccountId
        ORDER BY ""{nameof(AccountSnapshotEntity.SequenceNumber)}"" DESC
        LIMIT 1;";

    private const string AggregateDeltasSql = $@"
        SELECT COALESCE(SUM(""{nameof(AccountJournalEntryEntity.AmountDelta)}""), 0)
        FROM ""{nameof(BalanceDbContext.JournalEntries)}""
        WHERE ""{nameof(AccountJournalEntryEntity.SourceAccountId)}"" = @AccountId 
          AND ""{nameof(AccountJournalEntryEntity.SequenceNumber)}"" > @LatestSnapshotSequence;";

    public async Task<decimal> ReconstructAvailableBalanceAsync(
        Guid accountId,
        DbConnection connection,
        DbTransaction transaction,
        CancellationToken cancellationToken = default)
    {
        // 1. Fetch the absolute latest snapshot tracking state metadata safely wrapped with cancellation boundaries
        CommandDefinition snapshotCommand = new(SnapshotSql, new { AccountId = accountId }, transaction, cancellationToken: cancellationToken);
        (long SequenceNumber, decimal SnapshotBalance)? snapshot = await connection.QuerySingleOrDefaultAsync<(long SequenceNumber, decimal SnapshotBalance)?>(snapshotCommand);

        long latestSnapshotSequence = snapshot?.SequenceNumber ?? 0;
        decimal baseBalance = snapshot?.SnapshotBalance ?? 0.00m;

        // 2. Fetch and aggregate all streaming delta variations recorded past the snapshot with cancellation mapping
        CommandDefinition deltasCommand = new(AggregateDeltasSql, new { AccountId = accountId, LatestSnapshotSequence = latestSnapshotSequence }, transaction, cancellationToken: cancellationToken);
        decimal streamingDeltas = await connection.QuerySingleAsync<decimal>(deltasCommand);

        // 3. Return the absolute formulated capacity valuation
        return baseBalance + streamingDeltas;
    }
}
