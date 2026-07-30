using MassTransit;
using IntegrationBus.SagaOrchestrator.Service.Sagas;
using IntegrationBus.CoreLedger.Contracts.Messages.Events;
using IntegrationBus.AccountBalance.Contracts.Messages.Commands;

namespace IntegrationBus.SagaOrchestrator.Service.Activities;

/// <summary>
/// State machine activity designed to dispatch the balance confirmation command once the core ledger record is safely written.
/// </summary>
public sealed class ConfirmAccountBalanceActivity(ITopicProducer<ConfirmAccountBalance> producer) :
    IStateMachineActivity<TransactionSagaInstance, WriteLedgerRecordPassed>
{
    /// <inheritdoc />
    public void Probe(ProbeContext context) => context.CreateScope("confirm-account-balance-activity");

    /// <inheritdoc />
    public void Accept(StateMachineVisitor visitor) => visitor.Visit(this);

    /// <summary>
    /// Executes the business logic block to transition from a technical ledger commit to actual accounting balance updates.
    /// </summary>
    /// <param name="context">The state machine behavior execution context containing the core saga state reference.</param>
    /// <param name="next">The next execution pipeline step supervisor reference.</param>
    public async Task Execute(
        BehaviorContext<TransactionSagaInstance, WriteLedgerRecordPassed> context,
        IBehavior<TransactionSagaInstance, WriteLedgerRecordPassed> next)
    {
        // Emit the command payload targeting the Accounting service to apply immutable double-entry records
        await producer.Produce(new ConfirmAccountBalance
        {
            TransactionId = context.Saga.CorrelationId,
            TimestampUtc = DateTime.UtcNow
        }, context.CancellationToken);

        // Advance to the next configured handler chain step inside the workflow behavior sequence
        await next.Execute(context);
    }

    /// <inheritdoc />
    public Task Faulted<TException>(
        BehaviorExceptionContext<TransactionSagaInstance, WriteLedgerRecordPassed, TException> context,
        IBehavior<TransactionSagaInstance, WriteLedgerRecordPassed> next) where TException : Exception
    {
        // Gracefully propagate activity exception metadata forward through the standard fault management pipeline
        return next.Faulted(context);
    }
}
