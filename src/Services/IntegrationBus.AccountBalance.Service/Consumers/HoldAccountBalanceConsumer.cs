using System.Data.Common;
using Dapper;
using IntegrationBus.AccountBalance.Contracts.Messages.Commands;
using IntegrationBus.AccountBalance.Contracts.Messages.Events;
using IntegrationBus.AccountBalance.Service.DbContexts;
using IntegrationBus.AccountBalance.Service.Entities;
using IntegrationBus.AccountBalance.Service.Enums;
using IntegrationBus.Contracts.Enums;
using MassTransit;
using Microsoft.EntityFrameworkCore;

namespace IntegrationBus.AccountBalance.Service.Consumers;

/// <summary>
/// Processes account balance reservation commands inside a transactional database boundary.
/// </summary>
public sealed class HoldAccountBalanceConsumer(
    BalanceDbContext dbContext,
    ILogger<HoldAccountBalanceConsumer> logger,
    ITopicProducer<HoldAccountBalancePassed> passedProducer,
    ITopicProducer<HoldAccountBalanceFailed> failedProducer) : IConsumer<HoldAccountBalance>
{
    private const string GetAccountsCurrencyMetadataSql = $@"
        SELECT ""{nameof(AccountEntity.Id)}"" AS Id, ""{nameof(AccountEntity.Currency)}"" AS CurrencyValue
        FROM ""{nameof(BalanceDbContext.Accounts)}""
        WHERE ""{nameof(AccountEntity.Id)}"" IN (@SourceAccountId, @TargetAccountId);";

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

    private const string MaxSequenceSql = $@"
        SELECT COALESCE(MAX(""{nameof(AccountJournalEntryEntity.SequenceNumber)}""), 0)
        FROM ""{nameof(BalanceDbContext.JournalEntries)}""
        WHERE ""{nameof(AccountJournalEntryEntity.SourceAccountId)}"" = @AccountId;";

    private const string InsertJournalSql = $@"
        INSERT INTO ""{nameof(BalanceDbContext.JournalEntries)}"" (
            ""{nameof(AccountJournalEntryEntity.SourceAccountId)}"",
            ""{nameof(AccountJournalEntryEntity.TargetAccountId)}"",
            ""{nameof(AccountJournalEntryEntity.SequenceNumber)}"",
            ""{nameof(AccountJournalEntryEntity.AmountDelta)}"",
            ""{nameof(AccountJournalEntryEntity.EntryType)}"",
            ""{nameof(AccountJournalEntryEntity.TransactionId)}"",
            ""{nameof(AccountJournalEntryEntity.TimestampUtc)}"")
        VALUES (@SourceAccountId, @TargetAccountId, @SequenceNumber, @AmountDelta, @EntryType, @TransactionId, @TimestampUtc);";

    /// <summary>
    /// Processes the inbound asset reservation by compiling historical ledger streams and appending a secure hold entry for the source account.
    /// </summary>
    public async Task Consume(ConsumeContext<HoldAccountBalance> context)
    {
        HoldAccountBalance message = context.Message;

        logger.LogInformation("Processing event-sourced balance hold for Tx: {TransactionId}, Source Account: {AccountFromId}, Target Account: {AccountToId}",
            message.TransactionId, message.AccountFromId, message.AccountToId);

        DbConnection connection = dbContext.Database.GetDbConnection();
        if (connection.State != System.Data.ConnectionState.Open)
        {
            await connection.OpenAsync(context.CancellationToken);
        }

        using DbTransaction transaction = await connection.BeginTransactionAsync(context.CancellationToken);

        try
        {
            // 1. Verify existence and extract strict currency records for both accounts in a single database roundtrip
            List<(Guid Id, int CurrencyValue)> accountMetadataList = (await connection.QueryAsync<(Guid Id, int CurrencyValue)>(
                GetAccountsCurrencyMetadataSql,
                new { SourceAccountId = message.AccountFromId, TargetAccountId = message.AccountToId },
                transaction)).ToList();

            (Guid Id, int CurrencyValue) sourceMetadata = accountMetadataList.FirstOrDefault(a => a.Id == message.AccountFromId);
            (Guid Id, int CurrencyValue) targetMetadata = accountMetadataList.FirstOrDefault(a => a.Id == message.AccountToId);

            if (sourceMetadata.Equals(default) || targetMetadata.Equals(default))
            {
                throw new InvalidOperationException(
                    $"Account validation failure. Ensure both source account '{message.AccountFromId}' and target account '{message.AccountToId}' exist within the system registration boundaries.");
            }

            Currency sourceCurrency = (Currency)sourceMetadata.CurrencyValue;
            Currency targetCurrency = (Currency)targetMetadata.CurrencyValue;

            // Enforce strict multi-account currency compatibility invariants
            if (sourceCurrency != targetCurrency)
            {
                throw new InvalidOperationException(
                    $"Inter-account currency mismatch. Source account operates under '{sourceCurrency}', but target account operates under '{targetCurrency}'. Multi-currency operations require explicit exchange mediators.");
            }

            if (sourceCurrency != message.Currency)
            {
                throw new InvalidOperationException(
                    $"Transaction currency mismatch. The accounts utilize '{sourceCurrency}', but the transaction payload requested '{message.Currency}'.");
            }

            // Fetch the absolute latest snapshot tracking state metadata for the source account
            (long SequenceNumber, decimal SnapshotBalance)? snapshot = await connection.QuerySingleOrDefaultAsync<(long SequenceNumber, decimal SnapshotBalance)?>(
                SnapshotSql, new { AccountId = message.AccountFromId }, transaction);

            long latestSnapshotSequence = snapshot?.SequenceNumber ?? 0;
            decimal baseBalance = snapshot?.SnapshotBalance ?? 0.00m;

            // Fetch and aggregate all streaming delta variations recorded past the active snapshot
            decimal streamingDeltas = await connection.QuerySingleAsync<decimal>(
                AggregateDeltasSql, new { AccountId = message.AccountFromId, LatestSnapshotSequence = latestSnapshotSequence }, transaction);

            // Compute real-time liquid balance capacity limits
            decimal currentAvailableBalance = baseBalance + streamingDeltas;
            if (currentAvailableBalance < message.Amount)
            {
                throw new InvalidOperationException($"Insufficient ledger funds. Available capacity: {currentAvailableBalance}, Requested reservation: {message.Amount}");
            }

            // Determine the next sequential index execution step for the source account stream
            long currentMaxSequence = await connection.QuerySingleAsync<long>(
                MaxSequenceSql, new { AccountId = message.AccountFromId }, transaction);
            long nextSequenceNumber = Math.Max(currentMaxSequence, latestSnapshotSequence) + 1;

            // Append the immutable reservation log entry directly into the operational event stream journal
            await connection.ExecuteAsync(InsertJournalSql, new
            {
                SourceAccountId = message.AccountFromId,
                TargetAccountId = message.AccountToId, // Persisting the destination mapping smoothly
                SequenceNumber = nextSequenceNumber,
                AmountDelta = -message.Amount,
                EntryType = (int)JournalEntryType.Hold,
                message.TransactionId,
                TimestampUtc = DateTime.UtcNow
            }, transaction);

            await transaction.CommitAsync(context.CancellationToken);

            logger.LogInformation("Successfully appended balance hold ledger event for Tx: {TransactionId} at sequence position {Sequence}",
                message.TransactionId, nextSequenceNumber);

            await passedProducer.Produce(new HoldAccountBalancePassed
            {
                TransactionId = message.TransactionId,
                HeldAt = DateTime.UtcNow
            }, context.CancellationToken);
        }
        catch (Exception ex)
        {
            await transaction.RollbackAsync(context.CancellationToken);

            logger.LogError(ex, "Immutable balance hold ingestion failed for Tx: {TransactionId}. Dispatching tracking failure event.", message.TransactionId);

            await failedProducer.Produce(new HoldAccountBalanceFailed
            {
                TransactionId = message.TransactionId,
                Reason = ex.Message,
                FailedAt = DateTime.UtcNow
            }, context.CancellationToken);
        }
    }
}
