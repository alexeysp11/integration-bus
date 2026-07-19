using MassTransit;
using IntegrationBus.AccountBalance.Service.Consumers;
using IntegrationBus.AccountBalance.Contracts.Messages.Commands;

HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);

// Read database connection string footprint to validate Definition of Done
string balanceDbConnection = builder.Configuration.GetConnectionString("BalanceDb")
    ?? throw new InvalidOperationException("BalanceDb connection string is missing.");

string kafkaConnectionString = builder.Configuration["Kafka:BootstrapServers"] ?? "localhost:9092";

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

        rider.UsingKafka((context, k) =>
        {
            k.Host(kafkaConnectionString);

            // Bind the incoming Kafka topic to our specific infrastructure consumer
            k.TopicEndpoint<HoldAccountBalance>(
                "account-balance-hold",
                "balance-service-group",
                e =>
                {
                    e.ConfigureConsumer<HoldAccountBalanceConsumer>(context);
                });
        });
    });
});

IHost host = builder.Build();
await host.RunAsync();
