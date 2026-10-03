using MassTransit;
using Serilog;
using IntegrationBus.CoreLedger.Service.Consumers;
using IntegrationBus.CoreLedger.Contracts.Messages.Events;
using IntegrationBus.CoreLedger.Contracts.Messages.Commands;
using IntegrationBus.CoreLedger.Service.Models;
using IntegrationBus.CoreLedger.Service.Activities;
using IntegrationBus.CoreLedger.Service.DbContexts;
using IntegrationBus.Contracts;
using IntegrationBus.Shared.Extensions;
using IntegrationBus.Shared.Security;
using Microsoft.EntityFrameworkCore;
using StackExchange.Redis;

try
{
    WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

    // Bootstrap logging layers immediately to track container structural allocation phases
    Log.Logger = new LoggerConfiguration()
        .ReadFrom.Configuration(builder.Configuration)
        .CreateLogger();

    builder.Logging.ClearProviders();
    builder.Logging.AddSerilog();

    builder.Services.AddDbContext<LedgerDbContext>(options =>
        options.UseNpgsql(builder.Configuration.GetConnectionString("LedgerDb")));

    builder.Services
        .AddTelemetryResource("integration-bus-core-ledger-service")
        .AddCoreMetrics()
        .AddMassTransitMetrics()
        .AddDistributedTracing();

    string kafkaConnectionString = builder.Configuration["Kafka:BootstrapServers"]
        ?? throw new InvalidOperationException("Kafka connection string is not specified");

    string redisConnectionString = builder.Configuration["Redis:ConnectionString"]
        ?? throw new InvalidOperationException("Redis connection string is not specified");

    builder.Services.AddSingleton<IConnectionMultiplexer>(
        _ => ConnectionMultiplexer.Connect(redisConnectionString));

    builder.Services.AddHealthChecks()
        .AddNpgSql(builder.Configuration.GetConnectionString("LedgerDb")!, name: "postgres")
        .AddKafka(config => config.BootstrapServers = kafkaConnectionString, name: "kafka")
        .AddRedis(redisConnectionString, name: "redis");

    // Infrastructure-level data masking pipeline: mirrors HMAC-hashed shadow copies of confidential fields
    // (account identifiers, balances) into Kafka security topics
    builder.Services.AddSingleton<IHmacMaskingService, HmacMaskingService>();
    builder.Services.AddSingleton<ISecurityTopicPublisher>(sp =>
        new KafkaSecurityTopicPublisher(kafkaConnectionString, sp.GetRequiredService<ILogger<KafkaSecurityTopicPublisher>>()));

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
                        e.UseFilter(new SensitiveDataMaskingFilter<WriteLedgerRecord>(
                            KafkaTopics.CoreLedgerRecordWrite,
                            context.GetRequiredService<IHmacMaskingService>(),
                            context.GetRequiredService<ISecurityTopicPublisher>()));
                        e.ConfigureConsumer<WriteLedgerRecordConsumer>(context);
                    });
            });
        });
    });

    WebApplication app = builder.Build();

    app.MapHealthChecks("/health");
    app.UseMetricsScraping();

    using (IServiceScope scope = app.Services.CreateScope())
    {
        LedgerDbContext dbContext = scope.ServiceProvider.GetRequiredService<LedgerDbContext>();
        await dbContext.Database.MigrateAsync();
    }

    await app.RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "Core Ledger service host terminated unexpectedly");
}
finally
{
    await Log.CloseAndFlushAsync();
}
