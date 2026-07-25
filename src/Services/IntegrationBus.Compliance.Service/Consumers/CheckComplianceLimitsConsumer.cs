using System.Data.Common;
using Dapper;
using IntegrationBus.Compliance.Contracts.Messages.Commands;
using IntegrationBus.Compliance.Contracts.Messages.Events;
using IntegrationBus.Compliance.Service.DbContexts;
using IntegrationBus.Compliance.Service.Entities;
using IntegrationBus.Compliance.Service.Enums;
using MassTransit;
using Microsoft.EntityFrameworkCore;

namespace IntegrationBus.Compliance.Service.Consumers;

/// <summary>
/// Handles incoming regulatory and anti-fraud compliance verification requests from the Saga Orchestrator.
/// </summary>
public sealed class CheckComplianceLimitsConsumer(
    ILogger<CheckComplianceLimitsConsumer> logger,
    ComplianceDbContext dbContext,
    ITopicProducer<CheckComplianceLimitsPassed> passedProducer,
    ITopicProducer<CheckComplianceLimitsFailed> failedProducer) : IConsumer<CheckComplianceLimits>
{
    /// <summary>
    /// Reusable parameterized SQL script to prevent allocations and string duplication.
    /// </summary>
    private const string InsertAuditSql = $@"
        INSERT INTO ""{nameof(ComplianceDbContext.ComplianceAudits)}"" (
            ""{nameof(ComplianceAuditEntity.Id)}"",
            ""{nameof(ComplianceAuditEntity.TransactionId)}"",
            ""{nameof(ComplianceAuditEntity.SourceAccountId)}"",
            ""{nameof(ComplianceAuditEntity.TargetAccountId)}"",
            ""{nameof(ComplianceAuditEntity.Amount)}"",
            ""{nameof(ComplianceAuditEntity.Currency)}"",
            ""{nameof(ComplianceAuditEntity.Status)}"",
            ""{nameof(ComplianceAuditEntity.FailureReason)}"",
            ""{nameof(ComplianceAuditEntity.CreatedAtUtc)}"")
        VALUES (@Id, @TransactionId, @SourceAccountId, @TargetAccountId, @Amount, @Currency, @Status, @FailureReason, @CreatedAtUtc);";

    /// <summary>
    /// Executes the compliance and velocity checks verification sequence simulation and dispatches the outcome event over Kafka.
    /// </summary>
    public async Task Consume(ConsumeContext<CheckComplianceLimits> context)
    {
        CheckComplianceLimits message = context.Message;

        logger.LogInformation(
            "Ingesting compliance limits verification pipeline for TransactionId: {TransactionId}",
            message.TransactionId);

        DbConnection connection = dbContext.Database.GetDbConnection();
        if (connection.State != System.Data.ConnectionState.Open)
        {
            await connection.OpenAsync(context.CancellationToken);
        }

        using DbTransaction transaction = await connection.BeginTransactionAsync(context.CancellationToken);

        try
        {
            // Log the audit record confirming that compliance validation successfully passed
            await connection.ExecuteAsync(InsertAuditSql, new
            {
                Id = Guid.NewGuid(),
                message.TransactionId,
                message.SourceAccountId,
                message.TargetAccountId,
                message.Amount,
                message.Currency,
                Status = ComplianceStatus.Passed,
                FailureReason = (string?)null,
                CreatedAtUtc = DateTime.UtcNow
            }, transaction);

            await transaction.CommitAsync(context.CancellationToken);

            logger.LogInformation("Successfully persisted and dispatched compliance passing event for TransactionId: {TransactionId}", message.TransactionId);

            await passedProducer.Produce(new CheckComplianceLimitsPassed
            {
                TransactionId = message.TransactionId,
                VerifiedAt = DateTime.UtcNow
            }, context.CancellationToken);
        }
        catch (Exception ex)
        {
            await transaction.RollbackAsync(context.CancellationToken);
            logger.LogError(ex, "Compliance verification execution boundary failed for TransactionId: {TransactionId}. Sending failure event.", message.TransactionId);

            using DbTransaction errorTransaction = await connection.BeginTransactionAsync(CancellationToken.None);
            try
            {
                await connection.ExecuteAsync(InsertAuditSql, new
                {
                    Id = Guid.NewGuid(),
                    message.TransactionId,
                    message.SourceAccountId,
                    message.TargetAccountId,
                    message.Amount,
                    message.Currency,
                    Status = ComplianceStatus.Failed,
                    FailureReason = ex.Message,
                    CreatedAtUtc = DateTime.UtcNow
                }, errorTransaction);

                await errorTransaction.CommitAsync(CancellationToken.None);
            }
            catch (Exception dbEx)
            {
                await errorTransaction.RollbackAsync(CancellationToken.None);
                logger.LogCritical(dbEx, "Fatal: Failed to write failure audit log to database for TransactionId: {TransactionId}", message.TransactionId);
            }

            await failedProducer.Produce(new CheckComplianceLimitsFailed
            {
                TransactionId = message.TransactionId,
                Reason = ex.Message
            }, context.CancellationToken);
        }
    }
}
