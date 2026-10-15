using System.Data.Common;
using Dapper;
using IntegrationBus.CoreLedger.Service.DbContexts;
using IntegrationBus.CoreLedger.Service.Entities;
using IntegrationBus.CoreLedger.Service.Models;
using MassTransit;
using Microsoft.EntityFrameworkCore;

namespace IntegrationBus.CoreLedger.Service.Activities;

/// <summary>
/// Executes the primary database transactional append log operations and manages its rollback compensation state.
/// </summary>
public sealed class WriteAuditTrailActivity(ILogger<WriteAuditTrailActivity> logger, LedgerDbContext dbContext)
    : IActivity<WriteAuditTrailArguments, WriteAuditTrailLog>
{
    private const string InsertIntoLedgerEntriesSql = $@"
        INSERT INTO ""{nameof(LedgerDbContext.LedgerEntries)}"" (
            ""{nameof(LedgerEntryEntity.TransactionId)}"",
            ""{nameof(LedgerEntryEntity.Amount)}"",
            ""{nameof(LedgerEntryEntity.CreatedAt)}"")
        VALUES (@TransactionId, @Amount, @CreatedAt)
        ON CONFLICT (""{nameof(LedgerEntryEntity.TransactionId)}"") DO NOTHING;";

    private const string DeleteLedgerEntrySql = $@"
        DELETE FROM ""{nameof(LedgerDbContext.LedgerEntries)}""
        WHERE ""{nameof(LedgerEntryEntity.TransactionId)}"" = @TransactionId;";
    
    /// <summary>
    /// Executes the persistent transactional database record allocation simulation.
    /// </summary>
    public async Task<ExecutionResult> Execute(ExecuteContext<WriteAuditTrailArguments> context)
    {
        long generatedEntryId = DateTime.UtcNow.Ticks;

        try
        {
            logger.LogInformation(
                "Courier Stage 1 | Database: ledger | Executing SQL: INSERT INTO LedgerEntries (TransactionId: {TransactionId}, EntryId: {EntryId}, Amount: {Amount})",
                context.Arguments.TransactionId,
                generatedEntryId,
                context.Arguments.Amount);

            // Extract the raw underlying DbConnection from EF Core context
            DbConnection connection = dbContext.Database.GetDbConnection();

            // Execute lightning-fast parameterized native query without EF Core entity state overhead
            await connection.ExecuteAsync(InsertIntoLedgerEntriesSql, new
            {
                TransactionId = context.Arguments.TransactionId,
                Amount = context.Arguments.Amount,
                CreatedAt = System.DateTime.UtcNow
            });

            // Complete step successfully and store execution metrics inside the stateless tracking log context
            return context.Completed(new WriteAuditTrailLog
            {
                TransactionId = context.Arguments.TransactionId,
                LedgerEntryId = generatedEntryId
            });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Fatal crash inside WriteAuditTrail.Execute. Executing explicit internal self-compensation.");
            return context.Faulted(ex);
        }
    }

    /// <summary>
    /// Compensates the primary transaction entry if a downstream activity within the routing slip lifecycle fails.
    /// </summary>
    public async Task<CompensationResult> Compensate(CompensateContext<WriteAuditTrailLog> context)
    {
        logger.LogWarning(
            "Courier Compensation Triggered | Database: ledger | Executing Rollback SQL: DELETE FROM LedgerEntries WHERE TransactionId = {TransactionId}",
            context.Log.TransactionId);

        // Remove the entry so a transaction whose routing slip rolled back leaves no ledger record behind
        DbConnection connection = dbContext.Database.GetDbConnection();
        await connection.ExecuteAsync(DeleteLedgerEntrySql, new { context.Log.TransactionId });

        return context.Compensated();
    }
}
