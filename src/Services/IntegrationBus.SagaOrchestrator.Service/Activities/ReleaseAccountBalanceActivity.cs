using MassTransit;
using IntegrationBus.SagaOrchestrator.Service.Sagas;
using IntegrationBus.Compliance.Contracts.Messages.Events;
using IntegrationBus.CoreLedger.Contracts.Messages.Events;
using IntegrationBus.AccountBalance.Contracts.Enums;
using IntegrationBus.AccountBalance.Contracts.Messages.Commands;
using IntegrationBus.AccountBalance.Contracts.Messages.Events;

namespace IntegrationBus.SagaOrchestrator.Service.Activities;

/// <summary>
/// Dispatches the compensating balance release command to Kafka when downstream compliance or ledger commitment failure occurs.
/// </summary>
public sealed class ReleaseAccountBalanceActivity(ILogger<ReleaseAccountBalanceActivity> logger, ITopicProducer<ReleaseAccountBalance> producer) :
    IStateMachineActivity<TransactionSagaInstance, CheckComplianceLimitsFailed>,
    IStateMachineActivity<TransactionSagaInstance, WriteLedgerRecordFailed>,
    IStateMachineActivity<TransactionSagaInstance, ConfirmAccountBalanceFailed>
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
        logger.LogWarning(
            "Compensation | Compliance rejected Tx: {TransactionId} ({Reason}); releasing the held balance",
            context.Saga.CorrelationId, context.Message.Reason);

        await SendReleaseCommandAsync(context.Saga, ReleaseAccountBalanceReason.ComplianceViolation, context.CancellationToken);
        await next.Execute(context);
    }

    /// <summary>
    /// Executes the compensation sequence when triggered by a Core Ledger Failure event.
    /// </summary>
    public async Task Execute(
        BehaviorContext<TransactionSagaInstance, WriteLedgerRecordFailed> context,
        IBehavior<TransactionSagaInstance, WriteLedgerRecordFailed> next)
    {
        logger.LogWarning(
            "Compensation | Ledger write failed for Tx: {TransactionId} ({Reason}); releasing the held balance",
            context.Saga.CorrelationId, context.Message.Reason);

        await SendReleaseCommandAsync(context.Saga, ReleaseAccountBalanceReason.LedgerWriteFailure, context.CancellationToken);
        await next.Execute(context);
    }

    public async Task Execute(BehaviorContext<TransactionSagaInstance, ConfirmAccountBalanceFailed> context, IBehavior<TransactionSagaInstance, ConfirmAccountBalanceFailed> next)
    {
        logger.LogWarning(
            "Compensation | Accounting confirmation failed for Tx: {TransactionId} ({Reason}); releasing the held balance",
            context.Saga.CorrelationId, context.Message.Reason);

        await SendReleaseCommandAsync(context.Saga, ReleaseAccountBalanceReason.AccountingConfirmationFailure, context.CancellationToken);
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

    public Task Faulted<TException>(
        BehaviorExceptionContext<TransactionSagaInstance, ConfirmAccountBalanceFailed, TException> context,
        IBehavior<TransactionSagaInstance, ConfirmAccountBalanceFailed> next) where TException : Exception
    {
        return next.Faulted(context);
    }

    /// <summary>
    /// Encapsulates the common internal message production logic to decouple core dispatch from generic wrappers.
    /// </summary>
    private async Task SendReleaseCommandAsync(TransactionSagaInstance saga, ReleaseAccountBalanceReason reason, CancellationToken cancellationToken)
    {
        await producer.Produce(new ReleaseAccountBalance
        {
            TransactionId = saga.CorrelationId,
            AccountId = saga.SourceAccountId,
            Amount = saga.Amount,
            Reason = reason
        }, cancellationToken);
    }
}
