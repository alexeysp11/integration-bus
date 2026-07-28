using IntegrationBus.AccountBalance.Contracts.Messages.Commands;
using MassTransit;
using IntegrationBus.AccountBalance.Service.DbContexts;
using IntegrationBus.AccountBalance.Service.Entities;
using Microsoft.EntityFrameworkCore;
using IntegrationBus.AccountBalance.Contracts.Messages.Events;

namespace IntegrationBus.AccountBalance.Service.Consumers;

/// <summary>
/// Asynchronously consumes bulk seeding command payloads from Kafka infrastructure to efficiently populate the balance data layer.
/// </summary>
public sealed class SeedAccountDatabaseBulkDataConsumer(
    ILogger<SeedAccountDatabaseBulkDataConsumer> logger,
    BalanceDbContext dbContext,
    ITopicProducer<SeedAccountDatabaseBulkDataPassed> passedProducer,
    ITopicProducer<SeedAccountDatabaseBulkDataFailed> failedProducer) : IConsumer<SeedAccountDatabaseBulkData>
{
    private const int DatabaseBatchSize = 10000;

    /// <summary>
    /// Orchestrates high-speed generation and transactional state initialization for non-duplicating account records.
    /// </summary>
    public async Task Consume(ConsumeContext<SeedAccountDatabaseBulkData> context)
    {
        int totalToGenerate = context.Message.RecordQuantity;
        if (totalToGenerate <= 0)
        {
            logger.LogWarning("Received bulk seed command with an invalid record quantity: {Quantity}. Execution aborted.", totalToGenerate);
            return;
        }

        logger.LogInformation("Starting bulk database seeding execution flow for {TotalCount} account records.", totalToGenerate);

        try
        {
            // Deactivate Change Tracking pipeline explicitly to eliminate memory leaks during large sequence operations
            dbContext.ChangeTracker.AutoDetectChangesEnabled = false;
            dbContext.ChangeTracker.QueryTrackingBehavior = QueryTrackingBehavior.NoTracking;

            List<AccountEntity> executionBatch = new(DatabaseBatchSize);
            Random randomEngine = new();
            DateTime currentUtcTime = DateTime.UtcNow;

            for (int i = 0; i < totalToGenerate; i++)
            {
                decimal randomizedWhole = randomEngine.Next(10, 10000);
                decimal randomizedFractional = randomEngine.Next(0, 100) / 100.00m;
                decimal accountBalance = randomizedWhole + randomizedFractional;

                int temporalOffsetHours = randomEngine.Next(0, 8760);
                DateTime historicalTimestamp = currentUtcTime.AddHours(-temporalOffsetHours);

                AccountEntity uniqueAccount = new()
                {
                    Id = Guid.NewGuid(),
                    Balance = accountBalance,
                    UpdatedAt = historicalTimestamp
                };

                executionBatch.Add(uniqueAccount);

                if (executionBatch.Count >= DatabaseBatchSize || i == totalToGenerate - 1)
                {
                    int currentBatchSize = executionBatch.Count;

                    await dbContext.Accounts.AddRangeAsync(executionBatch, context.CancellationToken);
                    await dbContext.SaveChangesAsync(context.CancellationToken);

                    logger.LogInformation("Successfully persisted a batch of {BatchSize} records. Progress: {Current}/{Total}.", currentBatchSize, i + 1, totalToGenerate);

                    executionBatch.Clear();
                    dbContext.ChangeTracker.Clear();
                }
            }

            SeedAccountDatabaseBulkDataPassed passedEvent = new();
            await passedProducer.Produce(passedEvent, context.CancellationToken);

            logger.LogInformation("Flawlessly completed database seeding operation for {TotalCount} accounts. Outcome event dispatched.", totalToGenerate);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "A critical runtime failure occurred while executing database batch ingestion for seeding operation.");
            SeedAccountDatabaseBulkDataFailed failedEvent = new()
            {
                FailureReason = ex.Message
            };
            await failedProducer.Produce(failedEvent, context.CancellationToken);
        }
    }
}
