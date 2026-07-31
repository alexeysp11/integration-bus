using System.Data.Common;
using Dapper;
using IntegrationBus.AccountBalance.Contracts.Messages.Commands;
using IntegrationBus.AccountBalance.Contracts.Messages.Events;
using IntegrationBus.AccountBalance.Service.DbContexts;
using IntegrationBus.AccountBalance.Service.Entities;
using IntegrationBus.AccountBalance.Service.Enums;
using MassTransit;
using Microsoft.EntityFrameworkCore;

namespace IntegrationBus.AccountBalance.Service.Consumers;

/// <summary>
/// Asynchronously consumes balance compensation commands, appending a cancellation ledger event to release locked assets.
/// </summary>
public sealed class ReleaseAccountBalanceConsumer(
    ILogger<ReleaseAccountBalanceConsumer> logger,
    BalanceDbContext dbContext,
    ITopicProducer<ReleaseAccountBalancePassed> passedProducer,
    ITopicProducer<ReleaseAccountBalanceFailed> failedProducer,
    ITopicProducer<ReleaseAccountBalanceSkipped> skippedProducer) : IConsumer<ReleaseAccountBalance>
{
    private const string FindHoldEntrySql = $@"
        SELECT
            ""{nameof(AccountJournalEntryEntity.SourceAccountId)}"",
            ""{nameof(AccountJournalEntryEntity.TargetAccountId)}"",
            ""{nameof(AccountJournalEntryEntity.AmountDelta)}""
        FROM ""{nameof(BalanceDbContext.JournalEntries)}""
        WHERE ""{nameof(AccountJournalEntryEntity.TransactionId)}"" = @TransactionId 
            AND ""{nameof(AccountJournalEntryEntity.EntryType)}"" = @HoldType
        LIMIT 1;";

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
    /// Processes the inbound balance release command by locating the active hold and appending a neutralizing cancellation record.
    /// </summary>
    public async Task Consume(ConsumeContext<ReleaseAccountBalance> context)
    {
        ReleaseAccountBalance message = context.Message;

        logger.LogInformation("Executing technical ledger compensation to release funds for Tx: {TransactionId}", message.TransactionId);

        DbConnection connection = dbContext.Database.GetDbConnection();
        if (connection.State != System.Data.ConnectionState.Open)
        {
            await connection.OpenAsync(context.CancellationToken);
        }

        using DbTransaction transaction = await connection.BeginTransactionAsync(context.CancellationToken);

        try
        {
            // 1. Locate the active historical hold to identify the original source account context boundaries
            (Guid SourceAccountId, Guid? TargetAccountId, decimal AmountDelta)? holdEntry = await connection.QuerySingleOrDefaultAsync<(Guid SourceAccountId, Guid? TargetAccountId, decimal AmountDelta)?>(
                FindHoldEntrySql, new { message.TransactionId, HoldType = (int)JournalEntryType.Hold }, transaction);

            if (holdEntry is null)
            {
                // Idempotency branch: If no hold exists, we commit smoothly and dispatch a skipped event to prevent saga hang-ups
                logger.LogWarning("Compensation skipped. No active asset hold log discovered for Tx: {TransactionId}", message.TransactionId);

                await transaction.CommitAsync(context.CancellationToken);

                await skippedProducer.Produce(new ReleaseAccountBalanceSkipped
                {
                    TransactionId = message.TransactionId,
                    SkippedAtUtc = DateTime.UtcNow
                }, context.CancellationToken);

                return;
            }

            Guid sourceAccountId = holdEntry.Value.SourceAccountId;
            decimal absoluteHoldAmount = Math.Abs(holdEntry.Value.AmountDelta);

            // 2. Determine the next sequential index execution step for the source account stream log
            long currentMaxSequence = await connection.QuerySingleAsync<long>(MaxSequenceSql, new { AccountId = sourceAccountId }, transaction);

            // 3. Append the positive cancellation delta to unlock and restore disposable capacity pool limits
            await connection.ExecuteAsync(InsertJournalSql, new
            {
                SourceAccountId = sourceAccountId,
                TargetAccountId = holdEntry.Value.TargetAccountId,
                SequenceNumber = currentMaxSequence + 1,
                AmountDelta = absoluteHoldAmount,
                EntryType = (int)JournalEntryType.Cancelled,
                message.TransactionId,
                TimestampUtc = DateTime.UtcNow
            }, transaction);

            await transaction.CommitAsync(context.CancellationToken);

            logger.LogInformation("Successfully rolled back asset hold for Tx: {TransactionId}. Funds released onto Account: {AccountId}",
                message.TransactionId, sourceAccountId);

            // Dispatch isolated Accounting success event back to the awaiting Saga workflow pipeline
            await passedProducer.Produce(new ReleaseAccountBalancePassed
            {
                TransactionId = message.TransactionId
            }, context.CancellationToken);
        }
        catch (Exception ex)
        {
            await transaction.RollbackAsync(context.CancellationToken);

            logger.LogError(ex, "Critical failure encountered during compensation mapping sequence for Tx: {TransactionId}", message.TransactionId);

            // Dispatch isolated Accounting failure event to let the Saga know that compensation broke down
            await failedProducer.Produce(new ReleaseAccountBalanceFailed
            {
                TransactionId = message.TransactionId,
                Reason = ex.Message,
                FailedAtUtc = DateTime.UtcNow
            }, context.CancellationToken);
        }
    }
}
