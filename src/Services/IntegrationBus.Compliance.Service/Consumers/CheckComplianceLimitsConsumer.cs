using MassTransit;
using IntegrationBus.Compliance.Contracts.Messages.Commands;
using IntegrationBus.Compliance.Contracts.Messages.Events;

namespace IntegrationBus.Compliance.Service.Consumers;

/// <summary>
/// Handles incoming regulatory and anti-fraud compliance verification requests from the Saga Orchestrator.
/// </summary>
public sealed class CheckComplianceLimitsConsumer(ILogger<CheckComplianceLimitsConsumer> logger) : IConsumer<CheckComplianceLimits>
{
    /// <summary>
    /// Executes the compliance and velocity checks verification sequence simulation.
    /// </summary>
    public async Task Consume(ConsumeContext<CheckComplianceLimits> context)
    {
        // Audit log simulating direct transactional database insert operation
        logger.LogInformation(
            "Database: compliance | Executing SQL: INSERT INTO ComplianceAudit (TransactionId, SourceAccountId, TargetAccountId, Amount, Currency) VALUES ({TransactionId}, {SourceAccount}, {TargetAccount}, {Amount}, {Currency})",
            context.Message.TransactionId,
            context.Message.SourceAccountId,
            context.Message.TargetAccountId,
            context.Message.Amount,
            context.Message.Currency);

        // Respond back to the Saga Orchestrator over Kafka mirroring the outcome pattern
        await context.RespondAsync(new CheckComplianceLimitsPassed
        {
            TransactionId = context.Message.TransactionId
        });
    }
}
