using Dapper;
using IntegrationBus.AccountBalance.Service.Configurations;
using IntegrationBus.AccountBalance.Service.DbContexts;
using IntegrationBus.AccountBalance.Service.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using System.Data.Common;

namespace IntegrationBus.AccountBalance.Service.BackgroundServices;

/// <summary>
/// Asynchronously monitors event-sourced journal streams and evaluates transaction density milestones to persist absolute balance checkpoints.
/// </summary>
public sealed class SnapshotGenerationEngine(
    ILogger<SnapshotGenerationEngine> logger,
    IOptions<SnapshotEngineOptions> options,
    IServiceScopeFactory scopeFactory) : BackgroundService
{
    private readonly int _sequenceThreshold = options.Value.SequenceThreshold;
    private readonly TimeSpan _executionInterval = options.Value.ExecutionInterval;

    private const string FindSnapshotCandidatesSql = $@"
        SELECT
            j.""{nameof(AccountJournalEntryEntity.SourceAccountId)}"" AS AccountId,
            COALESCE(MAX(s.""{nameof(AccountSnapshotEntity.SequenceNumber)}""), 0) AS LastSnapshotSequence,
            MAX(j.""{nameof(AccountJournalEntryEntity.SequenceNumber)}"") AS CurrentJournalSequence
        FROM ""{nameof(BalanceDbContext.JournalEntries)}"" j
        LEFT JOIN ""{nameof(BalanceDbContext.Snapshots)}"" s 
            ON j.""{nameof(AccountJournalEntryEntity.SourceAccountId)}"" = s.""{nameof(AccountSnapshotEntity.AccountId)}""
        GROUP BY j.""{nameof(AccountJournalEntryEntity.SourceAccountId)}""
        HAVING MAX(j.""{nameof(AccountJournalEntryEntity.SequenceNumber)}"") - COALESCE(MAX(s.""{nameof(AccountSnapshotEntity.SequenceNumber)}""), 0) >= @Threshold;";

    private const string CalculateUnsavedDeltaSql = $@"
        SELECT COALESCE(SUM(""{nameof(AccountJournalEntryEntity.AmountDelta)}""), 0)
        FROM ""{nameof(BalanceDbContext.JournalEntries)}""
        WHERE ""{nameof(AccountJournalEntryEntity.SourceAccountId)}"" = @AccountId
          AND ""{nameof(AccountJournalEntryEntity.SequenceNumber)}"" > @LastSnapshotSequence
          AND ""{nameof(AccountJournalEntryEntity.SequenceNumber)}"" <= @CurrentJournalSequence;";

    private const string GetLatestSnapshotBalanceSql = $@"
        SELECT ""{nameof(AccountSnapshotEntity.SnapshotBalance)}""
        FROM ""{nameof(BalanceDbContext.Snapshots)}""
        WHERE ""{nameof(AccountSnapshotEntity.AccountId)}"" = @AccountId
          AND ""{nameof(AccountSnapshotEntity.SequenceNumber)}"" = @LastSnapshotSequence
        LIMIT 1;";

    private const string InsertSnapshotSql = $@"
        INSERT INTO ""{nameof(BalanceDbContext.Snapshots)}"" (
            ""{nameof(AccountSnapshotEntity.AccountId)}"",
            ""{nameof(AccountSnapshotEntity.SequenceNumber)}"",
            ""{nameof(AccountSnapshotEntity.SnapshotBalance)}"",
            ""{nameof(AccountSnapshotEntity.CapturedAtUtc)}"")
        VALUES (@AccountId, @SequenceNumber, @SnapshotBalance, @TimestampUtc);";

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Snapshot Generation Engine worker initialized targeting sequence threshold: {Threshold}", _sequenceThreshold);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ProcessSnapshotEvaluationsAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "An unhandled exception crashed the active snapshot generation loop iteration context.");
            }

            await Task.Delay(_executionInterval, stoppingToken);
        }
    }

    /// <summary>
    /// Coordinates the localized lookup and incremental aggregation computation of missing journal sequence spaces.
    /// </summary>
    private async Task ProcessSnapshotEvaluationsAsync(CancellationToken cancellationToken)
    {
        using IServiceScope scope = scopeFactory.CreateScope();
        BalanceDbContext dbContext = scope.ServiceProvider.GetRequiredService<BalanceDbContext>();

        DbConnection connection = dbContext.Database.GetDbConnection();
        if (connection.State != System.Data.ConnectionState.Open)
        {
            await connection.OpenAsync(cancellationToken);
        }

        // 1. Discover all account profiles that breached the sequence delta volume limits
        List<(Guid AccountId, long LastSnapshotSequence, long CurrentJournalSequence)> candidates = (await connection.QueryAsync<(Guid AccountId, long LastSnapshotSequence, long CurrentJournalSequence)>(
            FindSnapshotCandidatesSql, new { Threshold = _sequenceThreshold })).ToList();

        if (candidates.Count == 0)
        {
            logger.LogDebug("No ledger account evaluation tracks crossed the structural checkpoint milestones.");
            return;
        }

        logger.LogInformation("Discovered {Count} account entities requiring absolute balance state compaction snapshots.", candidates.Count);

        foreach ((Guid AccountId, long LastSnapshotSequence, long CurrentJournalSequence) candidate in candidates)
        {
            using DbTransaction transaction = await connection.BeginTransactionAsync(cancellationToken);

            try
            {
                // 2. Fetch the historical starting balance from the last recorded checkpoint (or default to 0.00m)
                decimal baseBalance = 0.00m;
                if (candidate.LastSnapshotSequence > 0)
                {
                    baseBalance = await connection.QuerySingleOrDefaultAsync<decimal>(
                        GetLatestSnapshotBalanceSql,
                        new { candidate.AccountId, candidate.LastSnapshotSequence },
                        transaction);
                }

                // 3. Compute the uncompressed aggregated volume change made inside the open window gaps
                decimal windowDelta = await connection.QuerySingleAsync<decimal>(
                    CalculateUnsavedDeltaSql,
                    new { candidate.AccountId, candidate.LastSnapshotSequence, candidate.CurrentJournalSequence },
                    transaction);

                // 4. Formulate the total immutable new absolute balance snapshot milestone
                decimal finalSnapshotBalance = baseBalance + windowDelta;

                // 5. Persist the absolute valuation record into the historical storage logs
                await connection.ExecuteAsync(InsertSnapshotSql, new
                {
                    AccountId = candidate.AccountId,
                    SequenceNumber = candidate.CurrentJournalSequence,
                    SnapshotBalance = finalSnapshotBalance,
                    TimestampUtc = DateTime.UtcNow
                }, transaction);

                await transaction.CommitAsync(cancellationToken);

                logger.LogInformation("Successfully compacted ledger context for Account {AccountId}. Checkpoint saved at Seq: {Sequence} with Balance: {Balance}",
                    candidate.AccountId, candidate.CurrentJournalSequence, finalSnapshotBalance);
            }
            catch (Exception ex)
            {
                await transaction.RollbackAsync(cancellationToken);
                logger.LogError(ex, "Failed to compile execution state snapshot matrix updates for Account: {AccountId}", candidate.AccountId);
            }
        }
    }
}
