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
/// Asynchronously consumes ledger confirmation commands, applying immutable double-entry journal records to finalize a transfer.
/// </summary>
public sealed class ConfirmAccountBalanceConsumer(
    BalanceDbContext dbContext,
    ILogger<ConfirmAccountBalanceConsumer> logger,
    ITopicProducer<ConfirmAccountBalancePassed> passedProducer,
    ITopicProducer<ConfirmAccountBalanceFailed> failedProducer) : IConsumer<ConfirmAccountBalance>
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
    /// Processes the inbound financial confirmation request by validating the historic hold context and executing atomic double-entry logs.
    /// </summary>
    public async Task Consume(ConsumeContext<ConfirmAccountBalance> context)
    {
        ConfirmAccountBalance message = context.Message;

        logger.LogInformation("Confirming event-sourced balance allocation for Tx: {TransactionId}", message.TransactionId);

        DbConnection connection = dbContext.Database.GetDbConnection();
        if (connection.State != System.Data.ConnectionState.Open)
        {
            await connection.OpenAsync(context.CancellationToken);
        }

        using DbTransaction transaction = await connection.BeginTransactionAsync(context.CancellationToken);

        try
        {
            // 1. Locate the active historical hold record to secure transaction context validation boundaries
            (Guid SourceAccountId, Guid? TargetAccountId, decimal AmountDelta) holdEntry = await connection.QuerySingleOrDefaultAsync<(Guid SourceAccountId, Guid? TargetAccountId, decimal AmountDelta)?>(
                    FindHoldEntrySql, new { message.TransactionId, HoldType = (int)JournalEntryType.Hold }, transaction)
                ?? throw new InvalidOperationException($"No active balance reservation hold found for Transaction: {message.TransactionId}");
           
            Guid sourceAccountId = holdEntry.SourceAccountId;
            Guid targetAccountId = holdEntry.TargetAccountId
                ?? throw new InvalidOperationException($"Hold event state corrupted. Target counterparty account context missing for Tx: {message.TransactionId}");

            // Reconstruct absolute positive volume size from the negative hold delta
            decimal absoluteTransferAmount = Math.Abs(holdEntry.AmountDelta);

            // 2. Append the confirmation resolution entry into the SOURCE account stream log (Delta is 0.00 because assets are already deducted)
            long currentSourceMaxSeq = await connection.QuerySingleAsync<long>(MaxSequenceSql, new { AccountId = sourceAccountId }, transaction);

            await connection.ExecuteAsync(InsertJournalSql, new
            {
                SourceAccountId = sourceAccountId,
                TargetAccountId = targetAccountId,
                SequenceNumber = currentSourceMaxSeq + 1,
                AmountDelta = 0.00m,
                EntryType = (int)JournalEntryType.Confirmed,
                message.TransactionId,
                TimestampUtc = DateTime.UtcNow
            }, transaction);

            // 3. Append the deposit fulfillment entry into the TARGET account stream log (Positive delta increments liquid capacity)
            long currentTargetMaxSeq = await connection.QuerySingleAsync<long>(MaxSequenceSql, new { AccountId = targetAccountId }, transaction);

            await connection.ExecuteAsync(InsertJournalSql, new
            {
                SourceAccountId = targetAccountId,
                TargetAccountId = sourceAccountId,
                SequenceNumber = currentTargetMaxSeq + 1,
                AmountDelta = absoluteTransferAmount,
                EntryType = (int)JournalEntryType.Confirmed,
                message.TransactionId,
                TimestampUtc = DateTime.UtcNow
            }, transaction);

            await transaction.CommitAsync(context.CancellationToken);

            logger.LogInformation("Successfully finalized double-entry ledger event updates for Tx: {TransactionId}", message.TransactionId);

            // Dispatch isolated Accounting success event back to the awaiting Saga workflow pipeline
            await passedProducer.Produce(new ConfirmAccountBalancePassed
            {
                TransactionId = message.TransactionId,
                ConfirmedAtUtc = DateTime.UtcNow
            }, context.CancellationToken);
        }
        catch (Exception ex)
        {
            await transaction.RollbackAsync(context.CancellationToken);

            logger.LogError(ex, "Accounting ledger confirmation execution failed for Tx: {TransactionId}. Triggering failure event.", message.TransactionId);

            // Dispatch isolated Accounting failure event back to the awaiting Saga workflow pipeline
            await failedProducer.Produce(new ConfirmAccountBalanceFailed
            {
                TransactionId = message.TransactionId,
                Reason = ex.Message,
                FailedAtUtc = DateTime.UtcNow
            }, context.CancellationToken);
        }
    }
}
