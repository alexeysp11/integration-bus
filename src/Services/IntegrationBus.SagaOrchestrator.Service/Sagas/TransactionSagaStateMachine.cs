using IntegrationBus.AccountBalance.Contracts.Messages.Commands;
using IntegrationBus.AccountBalance.Contracts.Messages.Events;
using IntegrationBus.SagaOrchestrator.Contracts.Messages.Commands;
using MassTransit;

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
        InstanceState(x => x.CurrentState);

        // Bind incoming Kafka events/commands to the state machine correlation identifier
        Event(() => StartTransactionSaga, x => x.CorrelateById(context => context.Message.TransactionId));
        Event(() => HoldAccountBalancePassed, x => x.CorrelateById(context => context.Message.TransactionId));
        Event(() => HoldAccountBalanceFailed, x => x.CorrelateById(context => context.Message.TransactionId));

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
                .SendAsync(
                    new Uri("queue:account-balance-hold"), // Target Kafka topic/queue endpoint
                    context => context.Init<HoldAccountBalance>(new HoldAccountBalance
                    {
                        TransactionId = context.Saga.CorrelationId,
                        AccountId = context.Saga.SourceAccountId,
                        Amount = context.Saga.Amount
                    }))
                .TransitionTo(AwaitingAccountBalanceHold));

        During(AwaitingAccountBalanceHold,
            When(HoldAccountBalancePassed)
                .Then(context =>
                {
                    // Proceed to next step inside Issue #3 (Compliance)
                }),
            When(HoldAccountBalanceFailed)
                .Then(context =>
                {
                    // Handle failure boundary inside Issue #3
                }));
    }

    /// <summary>
    /// Gets the state definition representing that the saga is waiting for the Account Balance service to reserve funds.
    /// </summary>
    public State AwaitingAccountBalanceHold { get; private set; } = null!;

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
