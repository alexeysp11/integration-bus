using MassTransit;
using IntegrationBus.Compliance.Contracts.Messages.Commands;
using IntegrationBus.Compliance.Contracts.Messages.Events;

namespace IntegrationBus.Compliance.Service.Consumers;

/// <summary>
/// Handles incoming regulatory and anti-fraud compliance verification requests from the Saga Orchestrator.
/// </summary>
public sealed class CheckComplianceLimitsConsumer(
    ILogger<CheckComplianceLimitsConsumer> logger,
    ITopicProducer<CheckComplianceLimitsPassed> passedProducer,
    ITopicProducer<CheckComplianceLimitsFailed> failedProducer) : IConsumer<CheckComplianceLimits>
{
    /// <summary>
    /// Executes the compliance and velocity checks verification sequence simulation and dispatches the outcome event over Kafka.
    /// </summary>
    public async Task Consume(ConsumeContext<CheckComplianceLimits> context)
    {
        logger.LogInformation(
            "Ingesting compliance limits verification pipeline for TransactionId: {TransactionId}",
            context.Message.TransactionId);

        try
        {
            // Audit log simulating direct transactional database insert operation required for telemetry baseline
            logger.LogInformation(
                "Database: compliance | Executing SQL: INSERT INTO ComplianceAudit (TransactionId, SourceAccountId, TargetAccountId, Amount, Currency) VALUES ({TransactionId}, {SourceAccount}, {TargetAccount}, {Amount}, {Currency})",
                context.Message.TransactionId,
                context.Message.SourceAccountId,
                context.Message.TargetAccountId,
                context.Message.Amount,
                context.Message.Currency);

            await passedProducer.Produce(new CheckComplianceLimitsPassed
            {
                TransactionId = context.Message.TransactionId,
                VerifiedAt = DateTime.UtcNow
            }, context.CancellationToken);

            logger.LogInformation(
                "Successfully dispatched compliance passing event for TransactionId: {TransactionId}",
                context.Message.TransactionId);
        }
        catch (Exception ex)
        {
            logger.LogError(
                ex,
                "Compliance verification execution boundary failed for TransactionId: {TransactionId}",
                context.Message.TransactionId);

            await failedProducer.Produce(new CheckComplianceLimitsFailed
            {
                TransactionId = context.Message.TransactionId,
                Reason = ex.Message
            }, context.CancellationToken);
        }
    }
}
