using MassTransit;
using IntegrationBus.SagaOrchestrator.Service.Sagas;

HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);

// Configure MassTransit with Kafka transport footprint
builder.Services.AddMassTransit(x =>
{
    x.UsingInMemory((context, cfg) =>
    {
        cfg.ConfigureEndpoints(context);
    });

    // Establish baseline Kafka rider footprint required for Issue #2
    x.AddRider(rider =>
    {
        // Register the stateful saga state machine inside IoC container
        rider.AddSagaStateMachine<TransactionSagaStateMachine, TransactionSagaInstance>()
                .InMemoryRepository();

        // Bind saga consumers to listen to their respective Kafka topics
        rider.AddConsumersFromNamespaceContaining<TransactionSagaStateMachine>();

        rider.UsingKafka((context, k) =>
        {
            k.Host("localhost:9092"); // Default local Kafka broker address allocation

            // Explicitly map incoming Kafka topic endpoint to the Saga instance listener
            k.TopicEndpoint<IntegrationBus.SagaOrchestrator.Contracts.Messages.Commands.StartTransactionSaga>(
                "saga-transaction-start",
                "saga-orchestrator-group",
                e =>
                {
                    e.ConfigureSaga<TransactionSagaInstance>(context);
                });

            k.TopicEndpoint<IntegrationBus.AccountBalance.Contracts.Messages.Events.HoldAccountBalancePassed>(
                "account-balance-hold-passed",
                "saga-orchestrator-group",
                e =>
                {
                    e.ConfigureSaga<TransactionSagaInstance>(context);
                });

            k.TopicEndpoint<IntegrationBus.AccountBalance.Contracts.Messages.Events.HoldAccountBalanceFailed>(
                "account-balance-hold-failed",
                "saga-orchestrator-group",
                e =>
                {
                    e.ConfigureSaga<TransactionSagaInstance>(context);
                });
        });
    });
});

IHost host = builder.Build();
await host.RunAsync();
