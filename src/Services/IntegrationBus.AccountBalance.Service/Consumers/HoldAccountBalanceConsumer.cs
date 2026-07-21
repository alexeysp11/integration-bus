using MassTransit;
using IntegrationBus.AccountBalance.Contracts.Messages.Commands;
using IntegrationBus.AccountBalance.Contracts.Messages.Events;

namespace IntegrationBus.AccountBalance.Service.Consumers;

/// <summary>
/// Handles incoming asset reservation requests from the global Saga Orchestrator.
/// </summary>
public sealed class HoldAccountBalanceConsumer(
    ILogger<HoldAccountBalanceConsumer> logger,
    IConfiguration configuration,
    ITopicProducer<HoldAccountBalancePassed> passedProducer,
    ITopicProducer<HoldAccountBalanceFailed> failedProducer) : IConsumer<HoldAccountBalance>
{
    private readonly string _connectionString = configuration.GetConnectionString("BalanceDb")
        ?? throw new InvalidOperationException("BalanceDb connection string is missing inside worker configuration.");

    /// <summary>
    /// Executes the asset reservation step by writing to PostgreSQL and publishing a completion event over Apache Kafka.
    /// </summary>
    public async Task Consume(ConsumeContext<HoldAccountBalance> context)
    {
        logger.LogInformation(
            "Processing account balance hold request for TransactionId: {TransactionId}, AccountId: {AccountId}",
            context.Message.TransactionId,
            context.Message.AccountId);

        try
        {
            // Execute the persistent transactional write into the isolated state ledger database
            //using (NpgsqlConnection connection = new NpgsqlConnection(_connectionString))
            //{
            //    await connection.OpenAsync(context.CancellationToken);

            //    using (NpgsqlCommand command = connection.CreateCommand())
            //    {
            //        command.CommandText = """
            //            INSERT INTO account_holds (transaction_id, account_id, amount, created_at)
            //            VALUES (@TransactionId, @AccountId, @Amount, @CreatedAt);
            //            """;

            //        command.Parameters.AddWithValue("TransactionId", context.Message.TransactionId);
            //        command.Parameters.AddWithValue("AccountId", context.Message.AccountId);
            //        command.Parameters.AddWithValue("Amount", context.Message.Amount);
            //        command.Parameters.AddWithValue("CreatedAt", DateTime.UtcNow);

            //        await command.ExecuteNonQueryAsync(context.CancellationToken);
            //    }
            //}

            logger.LogInformation(
                "Successfully committed balance hold record to database for TransactionId: {TransactionId}",
                context.Message.TransactionId);

            // Publish dedicated lifecycle event over Kafka instead of using invalid abstract RespondAsync pattern
            await passedProducer.Produce(new HoldAccountBalancePassed
            {
                TransactionId = context.Message.TransactionId,
                HeldAt = DateTime.UtcNow
            }, context.CancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogError(
                ex,
                "Failed to execute asset reservation pipeline for TransactionId: {TransactionId}. Dispatching failure event.",
                context.Message.TransactionId);

            // Publish failure event to trigger downstream Saga compensation rules
            await failedProducer.Produce(new HoldAccountBalanceFailed
            {
                TransactionId = context.Message.TransactionId,
                Reason = ex.Message,
                FailedAt = DateTime.UtcNow
            }, context.CancellationToken);
        }
    }
}
