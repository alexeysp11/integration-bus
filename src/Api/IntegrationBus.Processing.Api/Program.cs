using MassTransit;
using Scalar.AspNetCore;
using Serilog;
using IntegrationBus.SagaOrchestrator.Contracts.Messages.Commands;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .Enrich.FromLogContext()
    .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] [{SourceContext}] {Message:lj}{NewLine}{Exception}")
    .CreateLogger();

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

// Inject Serilog provider infrastructure into internal dependency container
builder.Services.AddSerilog();

// Register controllers and native OpenAPI specification engine
builder.Services.AddControllers();
builder.Services.AddOpenApi();

// Initialize MassTransit memory core and target Kafka rider environment
builder.Services.AddMassTransit(x =>
{
    x.AddRider(rider =>
    {
        // Explicitly register the outbound producer footprint for the startup trigger command
        rider.AddProducer<StartTransactionSaga>("saga-transaction-start");

        rider.UsingKafka((context, k) =>
        {
            k.Host("localhost:9092");
        });
    });

    x.UsingInMemory((context, cfg) =>
    {
        cfg.ConfigureEndpoints(context);
    });
});

WebApplication app = builder.Build();

// Configure the HTTP request pipeline for development environments
if (app.Environment.IsDevelopment())
{
    // Generate the baseline openapi/v1.json specification file
    app.MapOpenApi();

    // Render the interactive Scalar UI reference layout mapped to the schema
    app.MapScalarApiReference();
}

app.UseHttpsRedirection();

// Map baseline endpoints to expose controllers routing
app.MapControllers();

app.Run();
