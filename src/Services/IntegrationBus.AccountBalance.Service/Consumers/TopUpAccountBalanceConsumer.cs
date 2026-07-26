using IntegrationBus.AccountBalance.Contracts.Messages.Events;
using IntegrationBus.AccountBalance.Contracts.Messages.Commands;
using MassTransit;

namespace IntegrationBus.AccountBalance.Service.Consumers;

/// <summary>
/// Processes asynchronous account balance replenishment commands delivered via Kafka 
/// and dispatches the execution outcome events.
/// </summary>
public sealed class TopUpAccountBalanceConsumer(
    ILogger<TopUpAccountBalanceConsumer> logger,
    ITopicProducer<TopUpAccountBalancePassed> passedProducer,
    ITopicProducer<TopUpAccountBalanceFailed> failedProducer) : IConsumer<TopUpAccountBalance>
{
    /// <summary>
    /// Consumes the balance top-up command, simulates database operations, 
    /// and streams the resulting status event back to Kafka.
    /// </summary>
    /// <param name="context">The MassTransit execution context containing the command payload.</param>
    public async Task Consume(ConsumeContext<TopUpAccountBalance> context)
    {
        TopUpAccountBalance message = context.Message;

        logger.LogInformation("Received balance replenishment command for Account: {AccountId}, Tx: {TransactionId}",
            message.AccountId, message.TransactionId);

        try
        {
            // TODO: Database persistence layer will be implemented under the 'feature/accounting-event-sourcing' task scope.
            // Bypassing active database writes for MVP telemetry and testing baseline initialization.
            logger.LogWarning("Database write skipped. Simulating successful ledger entry for Account: {AccountId}", message.AccountId);

            // Simulate minor processing latency
            await Task.Delay(10, context.CancellationToken);

            // Produce a success event back to Kafka
            await passedProducer.Produce(new TopUpAccountBalancePassed
            {
                TransactionId = message.TransactionId,
                AccountId = message.AccountId,
                Amount = message.Amount,
                CompletedAtUtc = DateTime.UtcNow
            }, context.CancellationToken);

            logger.LogInformation("Successfully processed top-up and dispatched success event for Tx: {TransactionId}",
                message.TransactionId);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to execute balance replenishment sequence for Tx: {TransactionId}",
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
