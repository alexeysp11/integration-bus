using MassTransit;
using IntegrationBus.SagaOrchestrator.Service.Sagas;
using IntegrationBus.Compliance.Contracts.Messages.Events;
using IntegrationBus.AccountBalance.Contracts.Messages.Commands;

namespace IntegrationBus.SagaOrchestrator.Service.Activities;

/// <summary>
/// Dispatches the compensating balance release command to Kafka when downstream compliance validation fails.
/// </summary>
public sealed class ReleaseAccountBalanceHoldActivity(ITopicProducer<ReleaseAccountBalance> producer)
    : IStateMachineActivity<TransactionSagaInstance, CheckComplianceLimitsFailed>
{
    public void Probe(ProbeContext context) => context.CreateScope("release-account-balance-hold-activity");

    public void Accept(StateMachineVisitor visitor) => visitor.Visit(this);

    public async Task Execute(
        BehaviorContext<TransactionSagaInstance, CheckComplianceLimitsFailed> context,
        IBehavior<TransactionSagaInstance, CheckComplianceLimitsFailed> next)
    {
        // Issue the compensation command directly to the balance service to unfreeze funds
        await producer.Produce(new ReleaseAccountBalance
        {
            TransactionId = context.Saga.CorrelationId,
            AccountId = context.Saga.SourceAccountId,
            Amount = context.Saga.Amount
        }, context.CancellationToken);

        await next.Execute(context);
    }

    public Task Faulted<TException>(
        BehaviorExceptionContext<TransactionSagaInstance, CheckComplianceLimitsFailed, TException> context,
        IBehavior<TransactionSagaInstance, CheckComplianceLimitsFailed> next)
        where TException : Exception
    {
        return next.Faulted(context);
    }
}
