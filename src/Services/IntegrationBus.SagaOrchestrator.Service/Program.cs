using MassTransit;
using Serilog;
using IntegrationBus.Contracts;
using IntegrationBus.AccountBalance.Contracts.Messages.Commands;
using IntegrationBus.AccountBalance.Contracts.Messages.Events;
using IntegrationBus.Compliance.Contracts.Messages.Commands;
using IntegrationBus.Compliance.Contracts.Messages.Events;
using IntegrationBus.CoreLedger.Contracts.Messages.Commands;
using IntegrationBus.CoreLedger.Contracts.Messages.Events;
using IntegrationBus.SagaOrchestrator.Contracts.Messages.Commands;
using IntegrationBus.SagaOrchestrator.Service.DbContexts;
using IntegrationBus.SagaOrchestrator.Service.Sagas;
using Microsoft.EntityFrameworkCore;

try
{
    HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);

    // Bootstrap logging layers immediately to track container structural allocation phases
    Log.Logger = new LoggerConfiguration()
        .ReadFrom.Configuration(builder.Configuration)
        .CreateLogger();

    builder.Logging.ClearProviders();
    builder.Logging.AddSerilog();

    // Register database context for saga state and outbox storage
    builder.Services.AddDbContext<SagaDbContext>(options =>
    {
        options.UseNpgsql(builder.Configuration.GetConnectionString("SagaDb"));
    });

    string kafkaConnectionString = builder.Configuration["Kafka:BootstrapServers"]
        ?? throw new InvalidOperationException("Kafka connection string is not specified");

    // Configure MassTransit with Kafka transport footprint
    builder.Services.AddMassTransit(x =>
    {
        // Configure Transactional Outbox and Consumer Inbox for idempotency
        x.AddEntityFrameworkOutbox<SagaDbContext>(o =>
        {
            o.UsePostgres();
            o.UseBusOutbox();
        });

        x.UsingInMemory((context, cfg) =>
        {
            cfg.ConfigureEndpoints(context);
        });

        // Establish baseline Kafka rider footprint required for Issue #2
        x.AddRider(rider =>
        {
            // Bind saga state machine to Entity Framework repository
            rider.AddSagaStateMachine<TransactionSagaStateMachine, TransactionSagaInstance>()
                 .EntityFrameworkRepository(r =>
                 {
                     r.ExistingDbContext<SagaDbContext>();
                     r.UsePostgres();
                 });

            // Bind saga consumers to listen to their respective Kafka topics
            rider.AddConsumersFromNamespaceContaining<TransactionSagaStateMachine>();

            rider.AddProducer<HoldAccountBalance>(KafkaTopics.AccountBalanceHold);
            rider.AddProducer<CheckComplianceLimits>(KafkaTopics.ComplianceLimitsCheck);
            rider.AddProducer<ReleaseAccountBalance>(KafkaTopics.AccountBalanceRelease);
            rider.AddProducer<WriteLedgerRecord>(KafkaTopics.CoreLedgerRecordWrite);
            rider.AddProducer<ConfirmAccountBalance>(KafkaTopics.AccountBalanceConfirm);

            rider.UsingKafka((context, k) =>
            {
                k.Host(kafkaConnectionString);

                // Repeat this pattern for EVERY topic endpoint inside the orchestrator
                k.TopicEndpoint<StartTransactionSaga>(
                    KafkaTopics.SagaTransactionStart,
                    "saga-orchestrator-group",
                    e =>
                    {
                        // Enable the Transactional Outbox / Consumer Inbox middleware filter for this specific topic
                        e.UseEntityFrameworkOutbox<SagaDbContext>(context);
                        e.ConfigureSaga<TransactionSagaInstance>(context);
                    });

                k.TopicEndpoint<HoldAccountBalancePassed>(
                    KafkaTopics.AccountBalanceHoldPassed,
                    "saga-orchestrator-group",
                    e =>
                    {
                        e.UseEntityFrameworkOutbox<SagaDbContext>(context);
                        e.ConfigureSaga<TransactionSagaInstance>(context);
                    });

                k.TopicEndpoint<HoldAccountBalanceFailed>(
                    KafkaTopics.AccountBalanceHoldFailed,
                    "saga-orchestrator-group",
                    e =>
                    {
                        e.UseEntityFrameworkOutbox<SagaDbContext>(context);
                        e.ConfigureSaga<TransactionSagaInstance>(context);
                    });

                k.TopicEndpoint<CheckComplianceLimitsPassed>(
                    KafkaTopics.ComplianceLimitsCheckPassed,
                    "saga-orchestrator-group",
                    e =>
                    {
                        e.UseEntityFrameworkOutbox<SagaDbContext>(context);
                        e.ConfigureSaga<TransactionSagaInstance>(context);
                    });

                k.TopicEndpoint<CheckComplianceLimitsFailed>(
                    KafkaTopics.ComplianceLimitsCheckFailed,
                    "saga-orchestrator-group",
                    e =>
                    {
                        e.UseEntityFrameworkOutbox<SagaDbContext>(context);
                        e.ConfigureSaga<TransactionSagaInstance>(context);
                    });

                k.TopicEndpoint<WriteLedgerRecordPassed>(
                    KafkaTopics.CoreLedgerRecordWritePassed,
                    "saga-orchestrator-group",
                    e =>
                    {
                        e.UseEntityFrameworkOutbox<SagaDbContext>(context);
                        e.ConfigureSaga<TransactionSagaInstance>(context);
                    });

                k.TopicEndpoint<WriteLedgerRecordFailed>(
                    KafkaTopics.CoreLedgerRecordWriteFailed,
                    "saga-orchestrator-group",
                    e =>
                    {
                        e.UseEntityFrameworkOutbox<SagaDbContext>(context);
                        e.ConfigureSaga<TransactionSagaInstance>(context);
                    });

                k.TopicEndpoint<ConfirmAccountBalancePassed>(
                    KafkaTopics.AccountBalanceConfirmPassed,
                    "saga-orchestrator-group",
                    e =>
                    {
                        e.UseEntityFrameworkOutbox<SagaDbContext>(context);
                        e.ConfigureSaga<TransactionSagaInstance>(context);
                    });

                k.TopicEndpoint<ConfirmAccountBalanceFailed>(
                    KafkaTopics.AccountBalanceConfirmFailed,
                    "saga-orchestrator-group",
                    e =>
                    {
                        e.UseEntityFrameworkOutbox<SagaDbContext>(context);
                        e.ConfigureSaga<TransactionSagaInstance>(context);
                    });
            });
        });
    });

    IHost host = builder.Build();

    using (IServiceScope scope = host.Services.CreateScope())
    {
        SagaDbContext dbContext = scope.ServiceProvider.GetRequiredService<SagaDbContext>();
        await dbContext.Database.MigrateAsync();
    }

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
