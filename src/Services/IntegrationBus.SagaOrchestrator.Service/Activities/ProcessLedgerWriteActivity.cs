using MassTransit;
using IntegrationBus.SagaOrchestrator.Service.Sagas;
using IntegrationBus.Compliance.Contracts.Messages.Events;
using IntegrationBus.CoreLedger.Contracts.Messages.Commands;
using IntegrationBus.Contracts.Enums;

namespace IntegrationBus.SagaOrchestrator.Service.Activities;

/// <summary>
/// Dispatches the initiation command to the Core Ledger service to trigger its internal Courier Routing Slip pipeline.
/// </summary>
public sealed class ProcessLedgerWriteActivity(ITopicProducer<WriteLedgerRecord> producer)
    : IStateMachineActivity<TransactionSagaInstance, CheckComplianceLimitsPassed>
{
    public void Probe(ProbeContext context) => context.CreateScope("process-ledger-write-activity");

    public void Accept(StateMachineVisitor visitor) => visitor.Visit(this);

    public async Task Execute(
        BehaviorContext<TransactionSagaInstance, CheckComplianceLimitsPassed> context,
        IBehavior<TransactionSagaInstance, CheckComplianceLimitsPassed> next)
    {
        // Map directly to your actual contract model from the screenshot
        await producer.Produce(new WriteLedgerRecord
        {
            TransactionId = context.Saga.CorrelationId,
            SourceAccountId = context.Saga.SourceAccountId,
            TargetAccountId = context.Saga.TargetAccountId,
            Amount = context.Saga.Amount,
            Currency = (Currency)context.Saga.CurrencyId
        }, context.CancellationToken);

        await next.Execute(context);
    }

    public Task Faulted<TException>(
        BehaviorExceptionContext<TransactionSagaInstance, CheckComplianceLimitsPassed, TException> context,
        IBehavior<TransactionSagaInstance, CheckComplianceLimitsPassed> next)
        where TException : Exception
    {
        return next.Faulted(context);
    }
}
