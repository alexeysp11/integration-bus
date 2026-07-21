using MassTransit;
using IntegrationBus.SagaOrchestrator.Service.Sagas;
using IntegrationBus.SagaOrchestrator.Contracts.Messages.Commands;
using IntegrationBus.AccountBalance.Contracts.Messages.Commands;

namespace IntegrationBus.SagaOrchestrator.Service.Activities;

/// <summary>
/// Handles the outbound Kafka message dispatch to the Account Balance service via constructor dependency injection.
/// </summary>
public sealed class HoldAccountBalanceActivity(ITopicProducer<HoldAccountBalance> producer)
    : IStateMachineActivity<TransactionSagaInstance, StartTransactionSaga>
{
    public void Probe(ProbeContext context) => context.CreateScope("hold-account-balance-activity");

    public void Accept(StateMachineVisitor visitor) => visitor.Visit(this);

    public async Task Execute(
        BehaviorContext<TransactionSagaInstance, StartTransactionSaga> context,
        IBehavior<TransactionSagaInstance, StartTransactionSaga> next)
    {
        // Produce the domain holding command directly into the designated Apache Kafka topic partition
        await producer.Produce(new HoldAccountBalance
        {
            TransactionId = context.Saga.CorrelationId,
            AccountId = context.Saga.SourceAccountId,
            Amount = context.Saga.Amount
        }, context.CancellationToken);

        // Forward execution sequence to the subsequent state machine pipeline filters
        await next.Execute(context);
    }

    public Task Faulted<TException>(
        BehaviorExceptionContext<TransactionSagaInstance, StartTransactionSaga, TException> context,
        IBehavior<TransactionSagaInstance, StartTransactionSaga> next)
        where TException : Exception
    {
        return next.Faulted(context);
    }
}
