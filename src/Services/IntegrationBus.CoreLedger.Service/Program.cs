using MassTransit;
using Serilog;
using IntegrationBus.CoreLedger.Service.Consumers;
using IntegrationBus.CoreLedger.Contracts.Messages.Events;
using IntegrationBus.CoreLedger.Contracts.Messages.Commands;
using IntegrationBus.CoreLedger.Service.Models;
using IntegrationBus.CoreLedger.Service.Activities;
using IntegrationBus.CoreLedger.Service.DbContexts;
using Microsoft.EntityFrameworkCore;
using IntegrationBus.Contracts;

try
{
    HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);

    // Bootstrap logging layers immediately to track container structural allocation phases
    Log.Logger = new LoggerConfiguration()
        .ReadFrom.Configuration(builder.Configuration)
        .CreateLogger();

    builder.Logging.ClearProviders();
    builder.Logging.AddSerilog();

    builder.Services.AddDbContext<LedgerDbContext>(options =>
        options.UseNpgsql(builder.Configuration.GetConnectionString("LedgerDb")));

    string kafkaConnectionString = builder.Configuration["Kafka:BootstrapServers"]
        ?? throw new InvalidOperationException("Kafka connection string is not specified");

    builder.Services.AddMassTransit(x =>
    {
        x.AddConsumer<LedgerRoutingSlipEventConsumer>();

        // Register Courier routing slip activities inside the dependency container
        x.AddActivity<WriteAuditTrailActivity, WriteAuditTrailArguments, WriteAuditTrailLog>();
        x.AddActivity<UpdateCacheActivity, UpdateCacheArguments, UpdateCacheLog>();
        x.AddExecuteActivity<PublishLedgerCommittedActivity, PublishLedgerCommittedArguments>();

        // Configure the local high-performance memory transit bus for sub-transaction execution
        x.UsingInMemory((context, cfg) =>
        {
            cfg.ReceiveEndpoint("ledger-routing-slip-events", e =>
            {
                e.ConfigureConsumer<LedgerRoutingSlipEventConsumer>(context);
            });

            cfg.ConfigureEndpoints(context);
        });

        x.AddRider(rider =>
        {
            rider.AddConsumer<WriteLedgerRecordConsumer>();

            // Declare the final response producer so the slip can notify the Saga Orchestrator over Kafka
            rider.AddProducer<WriteLedgerRecordPassed>(KafkaTopics.CoreLedgerRecordWritePassed);
            rider.AddProducer<WriteLedgerRecordFailed>(KafkaTopics.CoreLedgerRecordWriteFailed);

            rider.UsingKafka((context, k) =>
            {
                k.Host(kafkaConnectionString);

                k.TopicEndpoint<WriteLedgerRecord>(
                    KafkaTopics.CoreLedgerRecordWrite,
                    "ledger-service-group",
                    e =>
                    {
                        e.ConfigureConsumer<WriteLedgerRecordConsumer>(context);
                    });
            });
        });
    });

    IHost host = builder.Build();

    using (IServiceScope scope = host.Services.CreateScope())
    {
        LedgerDbContext dbContext = scope.ServiceProvider.GetRequiredService<LedgerDbContext>();
        await dbContext.Database.MigrateAsync();
    }

    await host.RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "Core Ledger service host terminated unexpectedly");
}
finally
{
    await Log.CloseAndFlushAsync();
}
