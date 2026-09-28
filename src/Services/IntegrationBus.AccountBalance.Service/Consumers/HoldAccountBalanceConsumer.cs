using System.Data.Common;
using Dapper;
using IntegrationBus.AccountBalance.Contracts.Messages.Commands;
using IntegrationBus.AccountBalance.Contracts.Messages.Events;
using IntegrationBus.AccountBalance.Service.DbContexts;
using IntegrationBus.AccountBalance.Service.Entities;
using IntegrationBus.AccountBalance.Service.Enums;
using IntegrationBus.AccountBalance.Service.Providers;
using IntegrationBus.Contracts.Enums;
using MassTransit;
using Microsoft.EntityFrameworkCore;
using RedLockNet;

namespace IntegrationBus.AccountBalance.Service.Consumers;

/// <summary>
/// Processes account balance reservation commands inside a transactional database boundary.
/// </summary>
/// <remarks>
/// A Redis distributed lock keyed by the source account id serializes concurrent holds against the same account
/// across all replicas of this service, closing the read-balance/insert-hold TOCTOU race that a database transaction
/// alone (default Read Committed isolation) does not prevent. A duplicate <see cref="HoldAccountBalance"/> delivery
/// for a <c>TransactionId</c> that was already held is detected and replayed idempotently instead of double-debiting.
/// </remarks>
public sealed class HoldAccountBalanceConsumer(
    ILogger<HoldAccountBalanceConsumer> logger,
    BalanceDbContext dbContext,
    IAccountStateReconstructor stateReconstructor,
    IDistributedLockFactory lockFactory,
    ITopicProducer<HoldAccountBalancePassed> passedProducer,
    ITopicProducer<HoldAccountBalanceFailed> failedProducer) : IConsumer<HoldAccountBalance>
{
    private static readonly TimeSpan LockExpiry = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan LockWait = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan LockRetry = TimeSpan.FromMilliseconds(200);

    private const string GetAccountsCurrencyMetadataSql = $@"
        SELECT ""{nameof(AccountEntity.Id)}"" AS Id, ""{nameof(AccountEntity.Currency)}"" AS CurrencyValue
        FROM ""{nameof(BalanceDbContext.Accounts)}""
        WHERE ""{nameof(AccountEntity.Id)}"" IN (@SourceAccountId, @TargetAccountId);";

    private const string MaxSequenceSql = $@"
        SELECT COALESCE(MAX(""{nameof(AccountJournalEntryEntity.SequenceNumber)}""), 0)
        FROM ""{nameof(BalanceDbContext.JournalEntries)}""
        WHERE ""{nameof(AccountJournalEntryEntity.SourceAccountId)}"" = @AccountId;";

    private const string HoldEntryExistsSql = $@"
        SELECT 1
        FROM ""{nameof(BalanceDbContext.JournalEntries)}""
        WHERE ""{nameof(AccountJournalEntryEntity.TransactionId)}"" = @TransactionId
          AND ""{nameof(AccountJournalEntryEntity.EntryType)}"" = @EntryType
        LIMIT 1;";

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

        string lockResource = $"account-lock:{message.AccountFromId}";

        using IRedLock accountLock = await lockFactory.CreateLockAsync(
            lockResource, LockExpiry, LockWait, LockRetry, context.CancellationToken);

        if (!accountLock.IsAcquired)
        {
            logger.LogWarning(
                "Failed to acquire the distributed account lock for {AccountFromId} while holding Tx: {TransactionId}; another concurrent operation on the same account did not release it in time.",
                message.AccountFromId, message.TransactionId);

            await failedProducer.Produce(new HoldAccountBalanceFailed
            {
                TransactionId = message.TransactionId,
                Reason = "Could not acquire the distributed account lock in time; a concurrent hold on the same account is still in progress.",
                FailedAt = DateTime.UtcNow
            }, context.CancellationToken);

            return;
        }

        DbConnection connection = dbContext.Database.GetDbConnection();
        if (connection.State != System.Data.ConnectionState.Open)
        {
            await connection.OpenAsync(context.CancellationToken);
        }

        using DbTransaction transaction = await connection.BeginTransactionAsync(context.CancellationToken);

        try
        {
            // Idempotency guard: a redelivered command for a TransactionId that already produced a hold entry
            // is replayed without appending a second ledger mutation, protecting against Kafka at-least-once duplicates
            bool alreadyHeld = await connection.ExecuteScalarAsync<int?>(
                HoldEntryExistsSql,
                new { message.TransactionId, EntryType = (int)JournalEntryType.Hold },
                transaction) is not null;

            if (alreadyHeld)
            {
                await transaction.CommitAsync(context.CancellationToken);

                logger.LogInformation(
                    "Duplicate HoldAccountBalance delivery detected for Tx: {TransactionId}; replaying the success event without a second ledger mutation.",
                    message.TransactionId);

                await passedProducer.Produce(new HoldAccountBalancePassed
                {
                    TransactionId = message.TransactionId,
                    HeldAt = DateTime.UtcNow
                }, context.CancellationToken);

                return;
            }

            // 1. Verify existence and extract strict currency records for both accounts in a single database roundtrip
            List<(Guid Id, int CurrencyValue)> accountMetadataList = (await connection.QueryAsync<(Guid Id, int CurrencyValue)>(
                GetAccountsCurrencyMetadataSql,
                new { SourceAccountId = message.AccountFromId, TargetAccountId = message.AccountToId },
                transaction)).ToList();

            // Safely locate explicit metadata nodes utilizing nullable tuple semantics to prevent empty-guid collision bugs
            (Guid Id, int CurrencyValue)? sourceMetadata = accountMetadataList.Cast<(Guid Id, int CurrencyValue)?>().FirstOrDefault(a => a!.Value.Id == message.AccountFromId);
            (Guid Id, int CurrencyValue)? targetMetadata = accountMetadataList.Cast<(Guid Id, int CurrencyValue)?>().FirstOrDefault(a => a!.Value.Id == message.AccountToId);

            if (sourceMetadata is null || targetMetadata is null)
            {
                throw new InvalidOperationException(
                    $"Account validation failure. Ensure both source account '{message.AccountFromId}' and target account '{message.AccountToId}' exist within the system registration boundaries.");
            }

            Currency sourceCurrency = (Currency)sourceMetadata.Value.CurrencyValue;
            Currency targetCurrency = (Currency)targetMetadata.Value.CurrencyValue;

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

            // 2. Delegate real-time liquid balance capacity calculation directly to the isolated reconstruction engine
            decimal currentAvailableBalance = await stateReconstructor.ReconstructAvailableBalanceAsync(
                message.AccountFromId,
                connection,
                transaction,
                context.CancellationToken);

            if (currentAvailableBalance < message.Amount)
            {
                throw new InvalidOperationException($"Insufficient ledger funds. Available capacity: {currentAvailableBalance}, Requested reservation: {message.Amount}");
            }

            // 3. Determine the next sequential index execution step for the source account event stream
            long currentMaxSequence = await connection.QuerySingleAsync<long>(
                MaxSequenceSql, new { AccountId = message.AccountFromId }, transaction);

            // Since max sequence in journal dynamically advances past any existing snapshot indices, incrementing it is completely safe
            long nextSequenceNumber = currentMaxSequence + 1;

            // 4. Append the immutable reservation log entry directly into the operational event stream journal
            await connection.ExecuteAsync(InsertJournalSql, new
            {
                SourceAccountId = message.AccountFromId,
                TargetAccountId = message.AccountToId,
                SequenceNumber = nextSequenceNumber,
                AmountDelta = -message.Amount,
                EntryType = (int)JournalEntryType.Hold,
                message.TransactionId,
                TimestampUtc = DateTime.UtcNow
            }, transaction);

            await transaction.CommitAsync(context.CancellationToken);

            logger.LogInformation("Successfully appended balance hold ledger event for Tx: {TransactionId} at sequence position {Sequence}",
                message.TransactionId, nextSequenceNumber);

            // 5. Dispatch success event notification back onto the orchestrator coordination pipeline
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
