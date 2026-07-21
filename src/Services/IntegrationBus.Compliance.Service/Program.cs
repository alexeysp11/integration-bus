using MassTransit;
using Serilog;
using IntegrationBus.Compliance.Service.Consumers;
using IntegrationBus.Compliance.Contracts.Messages.Events;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .Enrich.FromLogContext()
    .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] [{SourceContext}] {Message:lj}{NewLine}{Exception}")
    .CreateLogger();

try
{
    HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);
    builder.Services.AddSerilog();

    string complianceDbConnection = builder.Configuration.GetConnectionString("ComplianceDb")
        ?? throw new InvalidOperationException("ComplianceDb connection string is missing.");

    string kafkaConnectionString = builder.Configuration["Kafka:BootstrapServers"] ?? "localhost:9092";

    builder.Services.AddMassTransit(x =>
    {
        x.UsingInMemory((context, cfg) => cfg.ConfigureEndpoints(context));

        x.AddRider(rider =>
        {
            rider.AddConsumer<CheckComplianceLimitsConsumer>();

            rider.AddProducer<CheckComplianceLimitsPassed>("compliance-limits-check-passed");
            rider.AddProducer<CheckComplianceLimitsFailed>("compliance-limits-check-failed");

            rider.UsingKafka((context, k) =>
            {
                k.Host(kafkaConnectionString);

                k.TopicEndpoint<IntegrationBus.Compliance.Contracts.Messages.Commands.CheckComplianceLimits>(
                    "compliance-limits-check",
                    "compliance-service-group",
                    e =>
                    {
                        e.ConfigureConsumer<CheckComplianceLimitsConsumer>(context);
                    });
            });
        });
    });

    IHost host = builder.Build();
    await host.RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "Compliance service host terminated unexpectedly");
}
finally
{
    await Log.CloseAndFlushAsync();
}
