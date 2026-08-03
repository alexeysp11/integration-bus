using MassTransit;
using Serilog;
using IntegrationBus.AccountBalance.Contracts.Messages.Commands;
using IntegrationBus.AccountBalance.Contracts.Messages.Events;
using IntegrationBus.AccountBalance.Service.BackgroundServices;
using IntegrationBus.AccountBalance.Service.Configurations;
using IntegrationBus.AccountBalance.Service.Consumers;
using IntegrationBus.AccountBalance.Service.DbContexts;
using IntegrationBus.Contracts;
using Microsoft.EntityFrameworkCore;
using IntegrationBus.AccountBalance.Service.Providers;

try
{
    HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);

    // Bootstrap logging layers immediately to track container structural allocation phases
    Log.Logger = new LoggerConfiguration()
        .ReadFrom.Configuration(builder.Configuration)
        .CreateLogger();

    builder.Logging.ClearProviders();
    builder.Logging.AddSerilog();

    builder.Services.AddDbContext<BalanceDbContext>(options =>
        options.UseNpgsql(builder.Configuration.GetConnectionString("BalanceDb")));

    builder.Services.AddScoped<IAccountStateReconstructor, AccountStateReconstructor>();

    builder.Services.Configure<SnapshotEngineOptions>(
        builder.Configuration.GetSection("SnapshotEngine"));

    builder.Services.AddHostedService<SnapshotGenerationEngine>();

    string kafkaConnectionString = builder.Configuration["Kafka:BootstrapServers"]
        ?? throw new InvalidOperationException("Kafka connection string is not specified");

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

    IHost host = builder.Build();

    using (IServiceScope scope = host.Services.CreateScope())
    {
        BalanceDbContext dbContext = scope.ServiceProvider.GetRequiredService<BalanceDbContext>();
        await dbContext.Database.MigrateAsync();
    }

    await host.RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "Account Balance service host terminated unexpectedly");
}
finally
{
    await Log.CloseAndFlushAsync();
}
