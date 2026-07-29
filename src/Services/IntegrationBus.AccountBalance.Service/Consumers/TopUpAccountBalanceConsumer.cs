using Dapper;
using IntegrationBus.AccountBalance.Contracts.Messages.Events;
using IntegrationBus.AccountBalance.Contracts.Messages.Commands;
using IntegrationBus.AccountBalance.Service.DbContexts;
using IntegrationBus.AccountBalance.Service.Entities;
using IntegrationBus.AccountBalance.Service.Enums;
using IntegrationBus.Contracts.Enums;
using Microsoft.EntityFrameworkCore;
using MassTransit;
using System.Data.Common;

namespace IntegrationBus.AccountBalance.Service.Consumers;

/// <summary>
/// Processes asynchronous account balance replenishment commands delivered via Kafka 
/// and dispatches the execution outcome events.
/// </summary>
/// <summary>
/// Asynchronously consumes account balance replenishment requests, utilizing append-only ledger mechanisms and optimistic concurrency.
/// </summary>
public sealed class TopUpAccountBalanceConsumer(
    BalanceDbContext dbContext,
    ILogger<TopUpAccountBalanceConsumer> logger,
    ITopicProducer<TopUpAccountBalancePassed> passedProducer,
    ITopicProducer<TopUpAccountBalanceFailed> failedProducer) : IConsumer<TopUpAccountBalance>
{
    private const string GetAccountMetadataSql = $@"
        SELECT ""{nameof(AccountEntity.Currency)}""
        FROM ""{nameof(BalanceDbContext.Accounts)}""
        WHERE ""{nameof(AccountEntity.Id)}"" = @AccountId;";

    private const string SnapshotSql = $@"
        SELECT ""{nameof(AccountSnapshotEntity.SequenceNumber)}""
        FROM ""{nameof(BalanceDbContext.Snapshots)}""
        WHERE ""{nameof(AccountSnapshotEntity.AccountId)}"" = @AccountId
        ORDER BY ""{nameof(AccountSnapshotEntity.SequenceNumber)}"" DESC
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
    /// Processes the inbound asset replenishment by validating metadata boundaries and appending a direct deposit journal ledger record.
    /// </summary>
    public async Task Consume(ConsumeContext<TopUpAccountBalance> context)
    {
        TopUpAccountBalance message = context.Message;

        logger.LogInformation("Received event-sourced balance replenishment command for Account: {AccountId}, Tx: {TransactionId}",
            message.AccountId, message.TransactionId);

        DbConnection connection = dbContext.Database.GetDbConnection();
        if (connection.State != System.Data.ConnectionState.Open)
        {
            await connection.OpenAsync(context.CancellationToken);
        }

        using DbTransaction transaction = await connection.BeginTransactionAsync(context.CancellationToken);

        try
        {
            // 1. Verify target account existence and enforce strict currency compatibility constraints
            int? accountCurrencyValue = await connection.QuerySingleOrDefaultAsync<int?>(
                    GetAccountMetadataSql, new { message.AccountId }, transaction)
                ?? throw new InvalidOperationException($"Target account {message.AccountId} was not found within the ledger context registration boundaries.");
            
            Currency accountCurrency = (Currency)accountCurrencyValue.Value;
            if (accountCurrency != message.Currency)
            {
                throw new InvalidOperationException(
                    $"Currency mismatch exception. Target account operates under '{accountCurrency}', but the replenishment request specified '{message.Currency}'.");
            }

            // 2. Fetch the latest sequence positioning checkpoint from snapshots
            long latestSnapshotSequence = await connection.QuerySingleOrDefaultAsync<long?>(
                SnapshotSql, new { message.AccountId }, transaction) ?? 0;

            // 3. Determine the next sequential index step inside the journal stream log
            long currentMaxSequence = await connection.QuerySingleAsync<long>(
                MaxSequenceSql, new { message.AccountId }, transaction);

            long nextSequenceNumber = Math.Max(currentMaxSequence, latestSnapshotSequence) + 1;

            // 4. Append the immutable direct deposit transaction entry into the event log journal
            await connection.ExecuteAsync(InsertJournalSql, new
            {
                SourceAccountId = message.AccountId,
                TargetAccountId = (Guid?)null, // No external counterparty destination context needed for basic direct top-up flows
                SequenceNumber = nextSequenceNumber,
                AmountDelta = message.Amount,
                EntryType = (int)JournalEntryType.DirectDeposit,
                message.TransactionId,
                TimestampUtc = DateTime.UtcNow
            }, transaction);

            await transaction.CommitAsync(context.CancellationToken);

            logger.LogInformation("Successfully appended direct deposit ledger event for Tx: {TransactionId} at sequence position {Sequence}",
                message.TransactionId, nextSequenceNumber);

            // Produce a success event back to Kafka
            await passedProducer.Produce(new TopUpAccountBalancePassed
            {
                TransactionId = message.TransactionId,
                AccountId = message.AccountId,
                Amount = message.Amount,
                CompletedAtUtc = DateTime.UtcNow
            }, context.CancellationToken);
        }
        catch (Exception ex)
        {
            await transaction.RollbackAsync(context.CancellationToken);

            logger.LogError(ex, "Failed to execute balance replenishment sequence for Tx: {TransactionId}. Dispatching failure token.",
                message.TransactionId);

            // Produce a failure event back to Kafka to allow orchestrators/systems to handle the error state
            await failedProducer.Produce(new TopUpAccountBalanceFailed
            {
                TransactionId = message.TransactionId,
                AccountId = message.AccountId,
                Reason = ex.Message,
                FailedAtUtc = DateTime.UtcNow
            }, context.CancellationToken);
        }
    }
}
