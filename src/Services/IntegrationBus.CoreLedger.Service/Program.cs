using MassTransit;
using Serilog;
using IntegrationBus.CoreLedger.Service.Consumers;

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
        x.UsingInMemory((context, cfg) => cfg.ConfigureEndpoints(context));

        x.AddRider(rider =>
        {
            rider.AddConsumer<WriteLedgerRecordConsumer>();

            rider.UsingKafka((context, k) =>
            {
                k.Host(kafkaConnectionString);

                k.TopicEndpoint<IntegrationBus.CoreLedger.Contracts.Messages.Commands.WriteLedgerRecord>(
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
