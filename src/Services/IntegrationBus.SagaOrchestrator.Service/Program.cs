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
using IntegrationBus.Contracts;

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

            rider.AddProducer<HoldAccountBalance>(KafkaTopics.AccountBalanceHold);
            rider.AddProducer<CheckComplianceLimits>(KafkaTopics.ComplianceLimitsCheck);
            rider.AddProducer<ReleaseAccountBalance>(KafkaTopics.AccountBalanceRelease);
            rider.AddProducer<WriteLedgerRecord>(KafkaTopics.CoreLedgerRecordWrite);

            rider.UsingKafka((context, k) =>
            {
                k.Host("localhost:9092"); // Default local Kafka broker address allocation

                // Explicitly map incoming Kafka topic endpoint to the Saga instance listener
                k.TopicEndpoint<StartTransactionSaga>(
                    KafkaTopics.SagaTransactionStart,
                    "saga-orchestrator-group",
                    e =>
                    {
                        e.ConfigureSaga<TransactionSagaInstance>(context);
                    });

                k.TopicEndpoint<HoldAccountBalancePassed>(
                    KafkaTopics.AccountBalanceHoldPassed,
                    "saga-orchestrator-group",
                    e =>
                    {
                        e.ConfigureSaga<TransactionSagaInstance>(context);
                    });

                k.TopicEndpoint<HoldAccountBalanceFailed>(
                    KafkaTopics.AccountBalanceHoldFailed,
                    "saga-orchestrator-group",
                    e =>
                    {
                        e.ConfigureSaga<TransactionSagaInstance>(context);
                    });

                k.TopicEndpoint<CheckComplianceLimitsPassed>(
                    KafkaTopics.ComplianceLimitsCheckPassed,
                    "saga-orchestrator-group",
                    e =>
                    {
                        e.ConfigureSaga<TransactionSagaInstance>(context);
                    });

                k.TopicEndpoint<CheckComplianceLimitsFailed>(
                    KafkaTopics.ComplianceLimitsCheckFailed,
                    "saga-orchestrator-group",
                    e =>
                    {
                        e.ConfigureSaga<TransactionSagaInstance>(context);
                    });

                k.TopicEndpoint<WriteLedgerRecordPassed>(
                    KafkaTopics.CoreLedgerRecordWritePassed,
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
