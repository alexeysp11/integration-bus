using MassTransit;
using Serilog;
using IntegrationBus.Compliance.Contracts.Messages.Events;
using IntegrationBus.Compliance.Service.Consumers;
using IntegrationBus.Compliance.Service.DbContexts;
using Microsoft.EntityFrameworkCore;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .Enrich.FromLogContext()
    .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] [{SourceContext}] {Message:lj}{NewLine}{Exception}")
    .CreateLogger();

try
{
    HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);
    builder.Services.AddSerilog();

    builder.Services.AddDbContext<ComplianceDbContext>(options =>
        options.UseNpgsql(builder.Configuration.GetConnectionString("ComplianceDb")));

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

    using (IServiceScope scope = host.Services.CreateScope())
    {
        ComplianceDbContext dbContext = scope.ServiceProvider.GetRequiredService<ComplianceDbContext>();
        await dbContext.Database.MigrateAsync();
    }

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
