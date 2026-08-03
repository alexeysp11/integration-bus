using MassTransit;
using Serilog;
using IntegrationBus.Compliance.Contracts.Messages.Events;
using IntegrationBus.Compliance.Service.Consumers;
using IntegrationBus.Compliance.Service.DbContexts;
using Microsoft.EntityFrameworkCore;
using IntegrationBus.Contracts;
using IntegrationBus.Compliance.Contracts.Messages.Commands;

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

    string kafkaConnectionString = builder.Configuration["Kafka:BootstrapServers"]
        ?? throw new InvalidOperationException("Kafka connection string is not specified");

    builder.Services.AddMassTransit(x =>
    {
        x.UsingInMemory((context, cfg) => cfg.ConfigureEndpoints(context));

        x.AddRider(rider =>
        {
            rider.AddConsumer<CheckComplianceLimitsConsumer>();

            rider.AddProducer<CheckComplianceLimitsPassed>(KafkaTopics.ComplianceLimitsCheckPassed);
            rider.AddProducer<CheckComplianceLimitsFailed>(KafkaTopics.ComplianceLimitsCheckFailed);

            rider.UsingKafka((context, k) =>
            {
                k.Host(kafkaConnectionString);

                k.TopicEndpoint<CheckComplianceLimits>(
                    KafkaTopics.ComplianceLimitsCheck,
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
