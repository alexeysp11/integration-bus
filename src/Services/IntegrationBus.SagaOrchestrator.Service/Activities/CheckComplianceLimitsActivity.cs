using MassTransit;
using IntegrationBus.SagaOrchestrator.Service.Sagas;
using IntegrationBus.AccountBalance.Contracts.Messages.Events;
using IntegrationBus.Compliance.Contracts.Messages.Commands;
using IntegrationBus.Contracts.Enums;

namespace IntegrationBus.SagaOrchestrator.Service.Activities;

/// <summary>
/// Handles the outbound Kafka message dispatch to the Compliance service via constructor dependency injection.
/// </summary>
public sealed class CheckComplianceLimitsActivity(ITopicProducer<CheckComplianceLimits> producer)
    : IStateMachineActivity<TransactionSagaInstance, HoldAccountBalancePassed>
{
    public void Probe(ProbeContext context) => context.CreateScope("check-compliance-limits-activity");

    public void Accept(StateMachineVisitor visitor) => visitor.Visit(this);

    public async Task Execute(
        BehaviorContext<TransactionSagaInstance, HoldAccountBalancePassed> context,
        IBehavior<TransactionSagaInstance, HoldAccountBalancePassed> next)
    {
        // Produce the security limits verification command directly into the designated Apache Kafka topic partition
        await producer.Produce(new CheckComplianceLimits
        {
            TransactionId = context.Saga.CorrelationId,
            SourceAccountId = context.Saga.SourceAccountId,
            TargetAccountId = context.Saga.TargetAccountId,
            Amount = context.Saga.Amount,
            Currency = (Currency)context.Saga.CurrencyId
        }, context.CancellationToken);

        // Forward execution sequence to the subsequent state machine pipeline filters
        await next.Execute(context);
    }

    public Task Faulted<TException>(
        BehaviorExceptionContext<TransactionSagaInstance, HoldAccountBalancePassed, TException> context,
        IBehavior<TransactionSagaInstance, HoldAccountBalancePassed> next)
        where TException : Exception
    {
        return next.Faulted(context);
    }
}
