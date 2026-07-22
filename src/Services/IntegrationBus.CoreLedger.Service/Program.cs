using MassTransit;
using Serilog;
using IntegrationBus.CoreLedger.Service.Consumers;
using IntegrationBus.CoreLedger.Contracts.Messages.Events;
using IntegrationBus.CoreLedger.Contracts.Messages.Commands;
using IntegrationBus.CoreLedger.Service.Models;
using IntegrationBus.CoreLedger.Service.Activities;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .Enrich.FromLogContext()
    .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] [{SourceContext}] {Message:lj}{NewLine}{Exception}")
    .CreateLogger();

try
{
    HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);
    builder.Services.AddSerilog();

    string ledgerDbConnection = builder.Configuration.GetConnectionString("LedgerDb")
        ?? throw new InvalidOperationException("LedgerDb connection string is missing.");

    string kafkaConnectionString = builder.Configuration["Kafka:BootstrapServers"] ?? "localhost:9092";

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
            rider.AddProducer<WriteLedgerRecordPassed>("core-ledger-record-write-passed");
            rider.AddProducer<WriteLedgerRecordFailed>("core-ledger-record-write-failed");

            rider.UsingKafka((context, k) =>
            {
                k.Host(kafkaConnectionString);

                k.TopicEndpoint<WriteLedgerRecord>(
                    "core-ledger-record-write",
                    "ledger-service-group",
                    e =>
                    {
                        e.ConfigureConsumer<WriteLedgerRecordConsumer>(context);
                    });
            });
        });
    });

    IHost host = builder.Build();
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
