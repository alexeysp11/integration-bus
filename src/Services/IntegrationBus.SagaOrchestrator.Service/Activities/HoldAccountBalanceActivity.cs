using MassTransit;
using IntegrationBus.SagaOrchestrator.Service.Sagas;
using IntegrationBus.SagaOrchestrator.Contracts.Messages.Commands;
using IntegrationBus.AccountBalance.Contracts.Messages.Commands;
using IntegrationBus.Contracts.Enums;

namespace IntegrationBus.SagaOrchestrator.Service.Activities;

/// <summary>
/// Handles the outbound Kafka message dispatch to the Account Balance service via constructor dependency injection.
/// </summary>
public sealed class HoldAccountBalanceActivity(ILogger<HoldAccountBalanceActivity> logger, ITopicProducer<HoldAccountBalance> producer)
    : IStateMachineActivity<TransactionSagaInstance, StartTransactionSaga>
{
    public void Probe(ProbeContext context) => context.CreateScope("hold-account-balance-activity");

    public void Accept(StateMachineVisitor visitor) => visitor.Visit(this);

    public async Task Execute(
        BehaviorContext<TransactionSagaInstance, StartTransactionSaga> context,
        IBehavior<TransactionSagaInstance, StartTransactionSaga> next)
    {
        logger.LogInformation(
            "Saga step 1/4 | Dispatching HoldAccountBalance for Tx: {TransactionId}, Source: {SourceAccountId}, Target: {TargetAccountId}, Amount: {Amount}",
            context.Saga.CorrelationId, context.Saga.SourceAccountId, context.Saga.TargetAccountId, context.Saga.Amount);

        // Produce the domain holding command directly into the designated Apache Kafka topic partition
        await producer.Produce(new HoldAccountBalance
        {
            TransactionId = context.Saga.CorrelationId,
            AccountFromId = context.Saga.SourceAccountId,
            AccountToId = context.Saga.TargetAccountId,
            Amount = context.Saga.Amount,
            Currency = (Currency)context.Saga.CurrencyId
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
