using MassTransit;
using IntegrationBus.SagaOrchestrator.Service.Sagas;
using IntegrationBus.Compliance.Contracts.Messages.Events;
using IntegrationBus.CoreLedger.Contracts.Messages.Events;
using IntegrationBus.AccountBalance.Contracts.Messages.Commands;

namespace IntegrationBus.SagaOrchestrator.Service.Activities;

/// <summary>
/// Dispatches the compensating balance release command to Kafka when downstream compliance or ledger commitment failure occurs.
/// </summary>
public sealed class ReleaseAccountBalanceActivity(ITopicProducer<ReleaseAccountBalance> producer) :
    IStateMachineActivity<TransactionSagaInstance, CheckComplianceLimitsFailed>,
    IStateMachineActivity<TransactionSagaInstance, WriteLedgerRecordFailed>
{
    public void Probe(ProbeContext context) => context.CreateScope("release-account-balance-hold-activity");

    public void Accept(StateMachineVisitor visitor) => visitor.Visit(this);

    /// <summary>
    /// Executes the compensation sequence when triggered by a Compliance Failure event.
    /// </summary>
    public async Task Execute(
        BehaviorContext<TransactionSagaInstance, CheckComplianceLimitsFailed> context,
        IBehavior<TransactionSagaInstance, CheckComplianceLimitsFailed> next)
    {
        await SendReleaseCommandAsync(context.Saga, context.CancellationToken);
        await next.Execute(context);
    }

    /// <summary>
    /// Executes the compensation sequence when triggered by a Core Ledger Failure event.
    /// </summary>
    public async Task Execute(
        BehaviorContext<TransactionSagaInstance, WriteLedgerRecordFailed> context,
        IBehavior<TransactionSagaInstance, WriteLedgerRecordFailed> next)
    {
        await SendReleaseCommandAsync(context.Saga, context.CancellationToken);
        await next.Execute(context);
    }

    public Task Faulted<TException>(
        BehaviorExceptionContext<TransactionSagaInstance, CheckComplianceLimitsFailed, TException> context,
        IBehavior<TransactionSagaInstance, CheckComplianceLimitsFailed> next) where TException : Exception
    {
        return next.Faulted(context);
    }

    public Task Faulted<TException>(
        BehaviorExceptionContext<TransactionSagaInstance, WriteLedgerRecordFailed, TException> context,
        IBehavior<TransactionSagaInstance, WriteLedgerRecordFailed> next) where TException : Exception
    {
        return next.Faulted(context);
    }

    /// <summary>
    /// Encapsulates the common internal message production logic to decouple core dispatch from generic wrappers.
    /// </summary>
    private async Task SendReleaseCommandAsync(TransactionSagaInstance saga, CancellationToken cancellationToken)
    {
        await producer.Produce(new ReleaseAccountBalance
        {
            TransactionId = saga.CorrelationId,
            AccountId = saga.SourceAccountId,
            Amount = saga.Amount
        }, cancellationToken);
    }
}
