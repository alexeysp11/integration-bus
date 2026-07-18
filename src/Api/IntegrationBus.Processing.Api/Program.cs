using MassTransit;
using Scalar.AspNetCore;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

// Register controllers and native OpenAPI specification engine
builder.Services.AddControllers();
builder.Services.AddOpenApi();

// Initialize MassTransit memory core and target Kafka rider environment
builder.Services.AddMassTransit(x =>
{
    x.AddRider(rider =>
    {
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
