using MassTransit;
using Serilog;
using IntegrationBus.AccountBalance.Contracts.Messages.Commands;
using IntegrationBus.AccountBalance.Contracts.Messages.Events;
using IntegrationBus.AccountBalance.Service.BackgroundServices;
using IntegrationBus.AccountBalance.Service.Configurations;
using IntegrationBus.AccountBalance.Service.Consumers;
using IntegrationBus.AccountBalance.Service.DbContexts;
using IntegrationBus.AccountBalance.Service.Providers;
using IntegrationBus.Contracts;
using IntegrationBus.Shared.Extensions;
using IntegrationBus.Shared.Security;
using Microsoft.EntityFrameworkCore;
using RedLockNet;
using RedLockNet.SERedis;
using RedLockNet.SERedis.Configuration;
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

    builder.Services.AddDbContext<BalanceDbContext>(options =>
        options.UseNpgsql(builder.Configuration.GetConnectionString("BalanceDb")));

    builder.Services
        .AddTelemetryResource("integration-bus-account-balance-service")
        .AddCoreMetrics()
        .AddMassTransitMetrics()
        .AddDistributedTracing();

    builder.Services.AddScoped<IAccountStateReconstructor, AccountStateReconstructor>();

    builder.Services.Configure<SnapshotEngineOptions>(
        builder.Configuration.GetSection("SnapshotEngine"));

    builder.Services.AddHostedService<SnapshotGenerationEngine>();

    string kafkaConnectionString = builder.Configuration["Kafka:BootstrapServers"]
        ?? throw new InvalidOperationException("Kafka connection string is not specified");

    string redisConnectionString = builder.Configuration["Redis:ConnectionString"]
        ?? throw new InvalidOperationException("Redis connection string is not specified");

    // Single shared multiplexer backs both the RedLock distributed lock factory and the cache health check
    builder.Services.AddSingleton<IConnectionMultiplexer>(
        _ => ConnectionMultiplexer.Connect(redisConnectionString));

    builder.Services.AddSingleton<IDistributedLockFactory>(sp =>
        RedLockFactory.Create([new RedLockMultiplexer(sp.GetRequiredService<IConnectionMultiplexer>())]));

    builder.Services.AddHealthChecks()
        .AddNpgSql(builder.Configuration.GetConnectionString("BalanceDb")!, name: "postgres")
        .AddKafka(config => config.BootstrapServers = kafkaConnectionString, name: "kafka")
        .AddRedis(redisConnectionString, name: "redis");

    // Infrastructure-level data masking pipeline: mirrors HMAC-hashed shadow copies of confidential fields
    // (account identifiers, balances) into Kafka security topics
    builder.Services.AddSingleton<IHmacMaskingService, HmacMaskingService>();
    builder.Services.AddSingleton<ISecurityTopicPublisher>(sp =>
        new KafkaSecurityTopicPublisher(kafkaConnectionString, sp.GetRequiredService<ILogger<KafkaSecurityTopicPublisher>>()));

    builder.Services.AddMassTransit(x =>
    {
        x.UsingInMemory((context, cfg) =>
        {
            cfg.ConfigureEndpoints(context);
        });

        x.AddRider(rider =>
        {
            // Automatically discover and register HoldAccountBalanceConsumer inside IoC container
            rider.AddConsumer<HoldAccountBalanceConsumer>();
            rider.AddConsumer<TopUpAccountBalanceConsumer>();
            rider.AddConsumer<SeedAccountDatabaseBulkDataConsumer>();
            rider.AddConsumer<ConfirmAccountBalanceConsumer>();
            rider.AddConsumer<ReleaseAccountBalanceConsumer>();

            rider.AddProducer<HoldAccountBalancePassed>(KafkaTopics.AccountBalanceHoldPassed);
            rider.AddProducer<HoldAccountBalanceFailed>(KafkaTopics.AccountBalanceHoldFailed);

            rider.AddProducer<TopUpAccountBalancePassed>(KafkaTopics.AccountBalanceTopUpPassed);
            rider.AddProducer<TopUpAccountBalanceFailed>(KafkaTopics.AccountBalanceTopUpFailed);

            rider.AddProducer<SeedAccountDatabaseBulkDataPassed>(KafkaTopics.AccountDatabaseSeedPassed);
            rider.AddProducer<SeedAccountDatabaseBulkDataFailed>(KafkaTopics.AccountDatabaseSeedFailed);

            rider.AddProducer<ReleaseAccountBalancePassed>(KafkaTopics.AccountBalanceReleasePassed);
            rider.AddProducer<ReleaseAccountBalanceSkipped>(KafkaTopics.AccountBalanceReleaseSkipped);
            rider.AddProducer<ReleaseAccountBalanceFailed>(KafkaTopics.AccountBalanceReleaseFailed);

            rider.AddProducer<ConfirmAccountBalancePassed>(KafkaTopics.AccountBalanceConfirmPassed);
            rider.AddProducer<ConfirmAccountBalanceFailed>(KafkaTopics.AccountBalanceConfirmFailed);

            rider.UsingKafka((context, k) =>
            {
                k.Host(kafkaConnectionString);

                // Bind the incoming Kafka topic to our specific infrastructure consumer
                k.TopicEndpoint<HoldAccountBalance>(
                    KafkaTopics.AccountBalanceHold,
                    "balance-service-group",
                    e =>
                    {
                        e.UseFilter(new SensitiveDataMaskingFilter<HoldAccountBalance>(
                            KafkaTopics.AccountBalanceHold,
                            context.GetRequiredService<IHmacMaskingService>(),
                            context.GetRequiredService<ISecurityTopicPublisher>()));
                        e.ConfigureConsumer<HoldAccountBalanceConsumer>(context);
                    });
                k.TopicEndpoint<TopUpAccountBalance>(
                    KafkaTopics.AccountBalanceTopUp,
                    "balance-service-group",
                    e =>
                    {
                        e.ConfigureConsumer<TopUpAccountBalanceConsumer>(context);
                    });
                k.TopicEndpoint<SeedAccountDatabaseBulkData>(
                    KafkaTopics.AccountDatabaseSeed,
                    "balance-service-group",
                    e =>
                    {
                        e.ConfigureConsumer<SeedAccountDatabaseBulkDataConsumer>(context);
                    });
                k.TopicEndpoint<ConfirmAccountBalance>(
                    KafkaTopics.AccountBalanceConfirm,
                    "balance-service-group",
                    e =>
                    {
                        e.ConfigureConsumer<ConfirmAccountBalanceConsumer>(context);
                    });
                k.TopicEndpoint<ReleaseAccountBalance>(
                    KafkaTopics.AccountBalanceRelease,
                    "balance-service-group",
                    e =>
                    {
                        e.ConfigureConsumer<ReleaseAccountBalanceConsumer>(context);
                    });
            });
        });
    });

    WebApplication app = builder.Build();

    app.MapHealthChecks("/health");
    app.UseMetricsScraping();

    using (IServiceScope scope = app.Services.CreateScope())
    {
        BalanceDbContext dbContext = scope.ServiceProvider.GetRequiredService<BalanceDbContext>();
        await dbContext.Database.MigrateAsync();
    }

    await app.RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "Account Balance service host terminated unexpectedly");
}
finally
{
    await Log.CloseAndFlushAsync();
}
