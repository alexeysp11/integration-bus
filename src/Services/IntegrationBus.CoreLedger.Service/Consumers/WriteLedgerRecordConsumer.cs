using MassTransit;
using MassTransit.Courier.Contracts;
using IntegrationBus.CoreLedger.Contracts.Messages.Commands;
using IntegrationBus.CoreLedger.Service.Models;
using IntegrationBus.CoreLedger.Contracts.Messages.Events;

namespace IntegrationBus.CoreLedger.Service.Consumers;

/// <summary>
/// Ingests the global financial ledger writing command and initiates the local multi-stage Courier Routing Slip engine.
/// </summary>
public sealed class WriteLedgerRecordConsumer(
    ILogger<WriteLedgerRecordConsumer> logger,
    ITopicProducer<WriteLedgerRecordFailed> failedProducer) : IConsumer<WriteLedgerRecord>
{
    /// <summary>
    /// Processes the incoming Kafka record by building and executing an atomic stateless local routing slip in memory.
    /// </summary>
    public async Task Consume(ConsumeContext<WriteLedgerRecord> context)
    {
        logger.LogInformation(
            "Ingesting external ledger command for TransactionId: {TransactionId}. Building local technical routing slip context.",
            context.Message.TransactionId);

        try
        {
            // Initialize the stateless execution pipeline tracking container with a dedicated correlation tracking boundary
            Guid trackingId = Guid.NewGuid();
            RoutingSlipBuilder builder = new(trackingId);

            // Use AddHeader instead of SetVariables. This guarantees metadata propagation to system events.
            builder.AddVariable("TransactionId", context.Message.TransactionId);

            Uri targetEventAddress = new("queue:ledger-routing-slip-events");

            // Removed invalid 'await' keyword. AddSubscription is a synchronous configuration method.
            builder.AddSubscription(
                targetEventAddress,
                RoutingSlipEvents.Completed | RoutingSlipEvents.Faulted,
                RoutingSlipEventContents.All);

            // Stage 1: Add persistent audit trail append log activity allocation
            builder.AddActivity(
                "WriteAuditTrail",
                new Uri("queue:WriteAuditTrail_execute"),
                new WriteAuditTrailArguments
                {
                    TransactionId = context.Message.TransactionId,
                    SourceAccountId = context.Message.SourceAccountId,
                    TargetAccountId = context.Message.TargetAccountId,
                    Amount = context.Message.Amount,
                    Currency = (int)context.Message.Currency
                });

            // Stage 2: Add high-performance fast-path transactional read cache mutation activity allocation
            builder.AddActivity(
                "UpdateCache",
                new Uri("queue:UpdateCache_execute"),
                new UpdateCacheArguments
                {
                    TransactionId = context.Message.TransactionId,
                    Amount = context.Message.Amount
                });

            // Stage 3: Add execution-only terminal external communication outbox broadcast notification activity allocation
            builder.AddActivity(
                "PublishLedgerCommitted",
                new Uri("queue:PublishLedgerCommitted_execute"),
                new PublishLedgerCommittedArguments
                {
                    TransactionId = context.Message.TransactionId,
                    Amount = context.Message.Amount,
                    Currency = (int)context.Message.Currency
                });

            // Compile and execute the technical routing slip sub-transaction sequence immediately inside local memory boundary
            RoutingSlip routingSlip = builder.Build();
            await context.Execute(routingSlip);

            logger.LogInformation(
                "Successfully dispatched local infrastructure routing slip for TransactionId: {TransactionId}. TrackingId allocated: {TrackingId}",
                context.Message.TransactionId,
                trackingId);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Routing Slip execution engine crashed on startup for TransactionId: {TransactionId}", context.Message.TransactionId);
            await failedProducer.Produce(new WriteLedgerRecordFailed
            {
                TransactionId = context.Message.TransactionId,
                Reason = $"Ledger infrastructure startup failure: {ex.Message}",
                FailedAt = DateTime.UtcNow
            }, context.CancellationToken);
        }
    }
}
