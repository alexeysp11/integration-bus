using MassTransit;
using MassTransit.Courier.Contracts;
using IntegrationBus.CoreLedger.Contracts.Messages.Events;

namespace IntegrationBus.CoreLedger.Service.Consumers;

public sealed class LedgerRoutingSlipEventConsumer(
    ILogger<LedgerRoutingSlipEventConsumer> logger,
    ITopicProducer<WriteLedgerRecordPassed> passedProducer,
    ITopicProducer<WriteLedgerRecordFailed> failedProducer) :
    IConsumer<RoutingSlipCompleted>,
    IConsumer<RoutingSlipFaulted>
{
    public async Task Consume(ConsumeContext<RoutingSlipCompleted> context)
    {
        if (context.Message.Variables.TryGetValue("TransactionId", out object? idValue) &&
            Guid.TryParse(idValue?.ToString(), out Guid transactionId))
        {
            logger.LogInformation(
                "Central Interceptor | Local routing slip completed successfully for TransactionId: {TransactionId}. Publishing Passed to Kafka.",
                transactionId);

            long technicalEntryId = DateTime.UtcNow.Ticks;

            await passedProducer.Produce(new WriteLedgerRecordPassed
            {
                TransactionId = transactionId,
                EntryId = technicalEntryId,
                CreatedAt = DateTime.UtcNow
            }, context.CancellationToken);
        }
        else
        {
            logger.LogCritical("Central Interceptor | Unable to process RoutingSlipCompleted. 'TransactionId' variable is missing or invalid.");
        }
    }

    public async Task Consume(ConsumeContext<RoutingSlipFaulted> context)
    {
        if (context.Message.Variables.TryGetValue("TransactionId", out object? idValue) &&
            Guid.TryParse(idValue?.ToString(), out Guid transactionId))
        {
            logger.LogError(
                "Central Interceptor | Local routing slip failed for TransactionId: {TransactionId}. Internal rollbacks completed. Publishing Failed to Kafka.",
                transactionId);

            await failedProducer.Produce(new WriteLedgerRecordFailed
            {
                TransactionId = transactionId,
                Reason = "Technical ledger execution pipeline failed inside the local memory boundary.",
                FailedAt = DateTime.UtcNow
            }, context.CancellationToken);
        }
        else
        {
            logger.LogCritical("Central Interceptor | Unable to process RoutingSlipFaulted. 'TransactionId' variable is missing or invalid.");
        }
    }
}
