using Asp.Versioning;
using IntegrationBus.AccountBalance.Contracts.Messages.Commands;
using IntegrationBus.Contracts;
using IntegrationBus.Processing.Api.Extensions;
using IntegrationBus.Processing.Api.Validation;
using IntegrationBus.SagaOrchestrator.Contracts.Messages.Commands;
using MassTransit;
using Microsoft.AspNetCore.Mvc;
using Scalar.AspNetCore;
using Serilog;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .Enrich.FromLogContext()
    .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] [{SourceContext}] {Message:lj}{NewLine}{Exception}")
    .CreateLogger();

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

// Inject Serilog provider infrastructure into internal dependency container
builder.Services.AddSerilog();

// Register controllers with an explicit, strongly-typed custom error layout handler
builder.Services.AddControllers()
    .ConfigureApiBehaviorOptions(options =>
    {
        options.InvalidModelStateResponseFactory = context => ValidationErrorResponseFactory.Create(context.ModelState);
    });
builder.Services.AddApiValidators();
builder.Services.AddOpenApi();

// Configure strict URL Semantic API Versioning
builder.Services.AddApiVersioning(options =>
{
    // If the client doesn't specify a version, fallback to the default one
    options.DefaultApiVersion = new ApiVersion(1, 0);
    options.AssumeDefaultVersionWhenUnspecified = true;

    // Report available API versions in response headers (e.g. api-supported-versions: 1.0)
    options.ReportApiVersions = true;
    
    // Enforce that versioning is read strictly from the URL segment template
    options.ApiVersionReader = new UrlSegmentApiVersionReader();
})
.AddApiExplorer(options =>
{
    options.GroupNameFormat = "'v'VVV";
    options.SubstituteApiVersionInUrl = true;
});

// Initialize MassTransit memory core and target Kafka rider environment
builder.Services.AddMassTransit(x =>
{
    x.AddRider(rider =>
    {
        // Explicitly register the outbound producer footprint for the startup trigger command
        rider.AddProducer<StartTransactionSaga>(KafkaTopics.SagaTransactionStart);
        rider.AddProducer<TopUpAccountBalance>(KafkaTopics.AccountBalanceTopUp);

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
