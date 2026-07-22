using MassTransit;
using Serilog;
using IntegrationBus.SagaOrchestrator.Service.Sagas;
using IntegrationBus.AccountBalance.Contracts.Messages.Commands;
using IntegrationBus.Compliance.Contracts.Messages.Commands;
using IntegrationBus.AccountBalance.Contracts.Messages.Events;
using IntegrationBus.SagaOrchestrator.Contracts.Messages.Commands;
using IntegrationBus.Compliance.Contracts.Messages.Events;
using IntegrationBus.CoreLedger.Contracts.Messages.Commands;
using IntegrationBus.CoreLedger.Contracts.Messages.Events;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .Enrich.FromLogContext()
    .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] [{SourceContext}] {Message:lj}{NewLine}{Exception}")
    .CreateLogger();

try
{
    HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);

    // Inject Serilog provider infrastructure into internal dependency container
    builder.Services.AddSerilog();

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

            rider.AddProducer<HoldAccountBalance>("account-balance-hold");
            rider.AddProducer<CheckComplianceLimits>("compliance-limits-check");
            rider.AddProducer<ReleaseAccountBalance>("account-balance-release");
            rider.AddProducer<WriteLedgerRecord>("core-ledger-record-write");

            rider.UsingKafka((context, k) =>
            {
                k.Host("localhost:9092"); // Default local Kafka broker address allocation

                // Explicitly map incoming Kafka topic endpoint to the Saga instance listener
                k.TopicEndpoint<StartTransactionSaga>(
                    "saga-transaction-start",
                    "saga-orchestrator-group",
                    e =>
                    {
                        e.ConfigureSaga<TransactionSagaInstance>(context);
                    });

                k.TopicEndpoint<HoldAccountBalancePassed>(
                    "account-balance-hold-passed",
                    "saga-orchestrator-group",
                    e =>
                    {
                        e.ConfigureSaga<TransactionSagaInstance>(context);
                    });

                k.TopicEndpoint<HoldAccountBalanceFailed>(
                    "account-balance-hold-failed",
                    "saga-orchestrator-group",
                    e =>
                    {
                        e.ConfigureSaga<TransactionSagaInstance>(context);
                    });

                k.TopicEndpoint<CheckComplianceLimitsPassed>(
                    "compliance-limits-check-passed",
                    "saga-orchestrator-group",
                    e =>
                    {
                        e.ConfigureSaga<TransactionSagaInstance>(context);
                    });

                k.TopicEndpoint<CheckComplianceLimitsFailed>(
                    "compliance-limits-check-failed",
                    "saga-orchestrator-group",
                    e =>
                    {
                        e.ConfigureSaga<TransactionSagaInstance>(context);
                    });

                k.TopicEndpoint<WriteLedgerRecordPassed>(
                    "core-ledger-record-write-passed",
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
}
catch (Exception ex)
{
    Log.Fatal(ex, "Saga Orchestrator service host terminated unexpectedly");
}
finally
{
    await Log.CloseAndFlushAsync();
}
