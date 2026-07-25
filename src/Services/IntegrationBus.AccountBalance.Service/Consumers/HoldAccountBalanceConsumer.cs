using MassTransit;
using Dapper;
using Microsoft.EntityFrameworkCore;
using IntegrationBus.AccountBalance.Contracts.Messages.Events;
using IntegrationBus.AccountBalance.Contracts.Messages.Commands;
using IntegrationBus.AccountBalance.Service.DbContexts;
using System.Data.Common;
using IntegrationBus.AccountBalance.Service.Entities;

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
    public async Task Consume(ConsumeContext<HoldAccountBalance> context)
    {
        HoldAccountBalance message = context.Message;

        logger.LogInformation("Processing balance hold for Tx: {TransactionId}, Account: {AccountId}",
            message.TransactionId, message.AccountId);

        DbConnection connection = dbContext.Database.GetDbConnection();
        if (connection.State != System.Data.ConnectionState.Open)
        {
            await connection.OpenAsync(context.CancellationToken);
        }

        using DbTransaction transaction = await connection.BeginTransactionAsync(context.CancellationToken);

        try
        {
            // Acquire a pessimistic row-level lock to prevent concurrent asset updates and race conditions.
            const string selectSql = $@"
                SELECT ""{nameof(AccountEntity.Balance)}""
                FROM ""{nameof(BalanceDbContext.Accounts)}"" 
                WHERE ""{nameof(AccountEntity.Id)}"" = @AccountId
                FOR UPDATE;";
            decimal? currentBalance = await connection.QuerySingleOrDefaultAsync<decimal?>(selectSql, new { message.AccountId }, transaction)
                ?? throw new InvalidOperationException($"Account {message.AccountId} not found.");
            if (currentBalance < message.Amount)
            {
                throw new InvalidOperationException($"Insufficient funds. Available: {currentBalance}, Requested: {message.Amount}");
            }

            // Deduct the requested asset allocation amount from the core account record
            const string updateBalanceSql = $@"
                UPDATE ""{nameof(BalanceDbContext.Accounts)}"" 
                SET
                    ""{nameof(AccountEntity.Balance)}"" = ""{nameof(AccountEntity.Balance)}"" - @Amount,
                    ""{nameof(AccountEntity.UpdatedAt)}"" = @UpdatedAt 
                WHERE ""{nameof(AccountEntity.Id)}"" = @AccountId;";
            await connection.ExecuteAsync(updateBalanceSql, new
            {
                message.Amount,
                UpdatedAt = DateTime.UtcNow,
                message.AccountId
            }, transaction);

            // Log the tracking history record to map the active stateful transaction reservation
            const string insertHoldSql = $@"
                INSERT INTO ""{nameof(BalanceDbContext.AccountHolds)}"" (
                    ""{nameof(AccountHoldEntity.TransactionId)}"",
                    ""{nameof(AccountHoldEntity.AccountId)}"",
                    ""{nameof(AccountHoldEntity.Amount)}"",
                    ""{nameof(AccountHoldEntity.CreatedAt)}"")
                VALUES (@TransactionId, @AccountId, @Amount, @CreatedAt);";
            await connection.ExecuteAsync(insertHoldSql, new
            {
                message.TransactionId,
                message.AccountId,
                message.Amount,
                CreatedAt = DateTime.UtcNow
            }, transaction);

            await transaction.CommitAsync(context.CancellationToken);

            logger.LogInformation("Successfully locked funds for Tx: {TransactionId}", message.TransactionId);

            await passedProducer.Produce(new HoldAccountBalancePassed
            {
                TransactionId = message.TransactionId,
                HeldAt = DateTime.UtcNow
            }, context.CancellationToken);
        }
        catch (Exception ex)
        {
            await transaction.RollbackAsync(context.CancellationToken);
            logger.LogError(ex, "Balance hold failed for Tx: {TransactionId}. Sending failure event.", message.TransactionId);
            await failedProducer.Produce(new HoldAccountBalanceFailed
            {
                TransactionId = message.TransactionId,
                Reason = ex.Message,
                FailedAt = DateTime.UtcNow
            }, context.CancellationToken);
        }
    }
}
