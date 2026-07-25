using MassTransit;
using Serilog;
using IntegrationBus.AccountBalance.Service.Consumers;
using IntegrationBus.AccountBalance.Contracts.Messages.Commands;
using IntegrationBus.AccountBalance.Contracts.Messages.Events;
using IntegrationBus.AccountBalance.Service.DbContexts;
using Microsoft.EntityFrameworkCore;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .Enrich.FromLogContext()
    .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] [{SourceContext}] {Message:lj}{NewLine}{Exception}")
    .CreateLogger();

try
{
    HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);

    // Inject Serilog provider infrastructure into internal dependency container
    builder.Services.AddSerilog();

    builder.Services.AddDbContext<BalanceDbContext>(options =>
        options.UseNpgsql(builder.Configuration.GetConnectionString("BalanceDb")));

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

            rider.AddProducer<HoldAccountBalancePassed>("account-balance-hold-passed");
            rider.AddProducer<HoldAccountBalanceFailed>("account-balance-hold-failed");

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
