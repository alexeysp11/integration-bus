using MassTransit;
using IntegrationBus.SagaOrchestrator.Service.Activities;
using IntegrationBus.SagaOrchestrator.Contracts.Messages.Commands;
using IntegrationBus.AccountBalance.Contracts.Messages.Events;
using IntegrationBus.Compliance.Contracts.Messages.Events;

namespace IntegrationBus.SagaOrchestrator.Service.Sagas;

/// <summary>
/// Declares the stateful transitions, triggers, and orchestration workflow logic for the transaction saga.
/// </summary>
public sealed class TransactionSagaStateMachine : MassTransitStateMachine<TransactionSagaInstance>
{
    /// <summary>
    /// Initializes a new instance of the <see cref="TransactionSagaStateMachine"/> class and binds the operational pipeline.
    /// </summary>
    public TransactionSagaStateMachine()
    {
        // Define the state tracking property matching the persistent instance
        InstanceState(x => x.CurrentState);

        // Bind incoming Kafka events and command messages to the saga correlation tracking boundaries
        Event(() => StartTransactionSaga, x => x.CorrelateById(context => context.Message.TransactionId));
        Event(() => HoldAccountBalancePassed, x => x.CorrelateById(context => context.Message.TransactionId));
        Event(() => HoldAccountBalanceFailed, x => x.CorrelateById(context => context.Message.TransactionId));
        Event(() => CheckComplianceLimitsPassed, x => x.CorrelateById(context => context.Message.TransactionId));
        Event(() => CheckComplianceLimitsFailed, x => x.CorrelateById(context => context.Message.TransactionId));

        Initially(
            When(StartTransactionSaga)
                .Then(context =>
                {
                    // Mutate state instance properties from the starting command payload
                    context.Saga.SourceAccountId = context.Message.SourceAccountId;
                    context.Saga.TargetAccountId = context.Message.TargetAccountId;
                    context.Saga.Amount = context.Message.Amount;
                    context.Saga.CurrencyId = (int)context.Message.Currency;
                    context.Saga.CreatedAt = DateTime.UtcNow;
                })
                .Activity(x => x.OfType<HoldAccountBalanceActivity>())
                .TransitionTo(AwaitingAccountBalanceHold));

        During(AwaitingAccountBalanceHold,
            When(HoldAccountBalancePassed)
                .Activity(x => x.OfType<CheckComplianceLimitsActivity>())
                .TransitionTo(AwaitingComplianceLimitsCheck),

            // If the balance cannot be held, the transaction is terminal-failed immediately (no rollback needed)
            When(HoldAccountBalanceFailed)
                .Then(context =>
                {
                    context.Saga.ErrorMessage = context.Message.Reason;
                })
                .TransitionTo(Failed));

        During(AwaitingComplianceLimitsCheck,
            // Compliance success advances the saga to launch the local multi-activity transaction ledger step
            When(CheckComplianceLimitsPassed)
                .Activity(x => x.OfType<ProcessLedgerWriteActivity>())
                .TransitionTo(AwaitingLedgerCommit),

            // Compliance failure triggers technical compensation sequence to roll back the locked account balances
            When(CheckComplianceLimitsFailed)
                .Then(context =>
                {
                    context.Saga.ErrorMessage = context.Message.Reason;
                })
                .Activity(x => x.OfType<ReleaseAccountBalanceHoldActivity>())
                .TransitionTo(Failed));
    }

    /// <summary>
    /// Gets the state definition representing that the saga is waiting for the Account Balance service to reserve funds.
    /// </summary>
    public State AwaitingAccountBalanceHold { get; private set; } = null!;

    /// <summary>
    /// Gets the state definition representing that the saga is waiting for the Compliance service to validate limits.
    /// </summary>
    public State AwaitingComplianceLimitsCheck { get; private set; } = null!;

    /// <summary>
    /// Gets the state definition representing that the saga is waiting for the Core Ledger courier routing slip to execute.
    /// </summary>
    public State AwaitingLedgerCommit { get; private set; } = null!;

    // TODO: add xml comment.
    public State Failed { get; private set; } = null!;

    /// <summary>
    /// Gets the trigger event configuration indicating that the compliance limits verification passed successfully.
    /// </summary>
    public Event<CheckComplianceLimitsPassed> CheckComplianceLimitsPassed { get; private set; } = null!;

    /// <summary>
    /// Gets the trigger event configuration indicating that the compliance limits verification failed.
    /// </summary>
    public Event<CheckComplianceLimitsFailed> CheckComplianceLimitsFailed { get; private set; } = null!;

    /// <summary>
    /// Gets the trigger event configuration for the transaction initialization command.
    /// </summary>
    public Event<StartTransactionSaga> StartTransactionSaga { get; private set; } = null!;

    /// <summary>
    /// Gets the trigger event configuration indicating that the balance hold was successfully completed.
    /// </summary>
    public Event<HoldAccountBalancePassed> HoldAccountBalancePassed { get; private set; } = null!;

    /// <summary>
    /// Gets the trigger event configuration indicating that the balance hold operation failed.
    /// </summary>
    public Event<HoldAccountBalanceFailed> HoldAccountBalanceFailed { get; private set; } = null!;
}
