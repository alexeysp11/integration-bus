using IntegrationBus.Gateway.Api.Authentication;
using IntegrationBus.Identity.Contracts;
using Serilog;
using IntegrationBus.Shared.Extensions;

try
{
    WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

    // Bootstrap logging layers immediately to track container structural allocation phases
    Log.Logger = new LoggerConfiguration()
        .ReadFrom.Configuration(builder.Configuration)
        .CreateLogger();

    builder.Services
        .AddTelemetryResource("integration-bus-gateway-api")
        .AddCoreMetrics()
        .AddHttpMetrics()
        .AddDistributedTracing();

    builder.Logging.ClearProviders();
    builder.Logging.AddSerilog();

    builder.Services.AddOpenApi();

    builder.Services.AddHealthChecks();

    IdentityOptions identity = builder.Configuration.GetSection("Identity").Get<IdentityOptions>() ?? new IdentityOptions();

    builder.Services.Configure<IdentityOptions>(builder.Configuration.GetSection("Identity"));
    if (identity.Enabled)
    {
        string identityGrpcAddress = !string.IsNullOrWhiteSpace(identity.GrpcAddress)
            ? identity.GrpcAddress
            : throw new InvalidOperationException("Identity:GrpcAddress is not specified");

        builder.Services.AddGrpcClient<TokenValidation.TokenValidationClient>(grpc => grpc.Address = new Uri(identityGrpcAddress));
    }

    builder.Services.AddReverseProxy()
        .LoadFromConfig(builder.Configuration.GetSection("ReverseProxy"));

    WebApplication app = builder.Build();

    if (app.Environment.IsDevelopment())
    {
        app.MapOpenApi();
    }

    app.UseHttpsRedirection();
    if (identity.Enabled)
    {
        app.UseMiddleware<TokenValidationMiddleware>();
    }
    app.MapReverseProxy();
    app.MapHealthChecks("/health");
    app.UseMetricsScraping();

    await app.RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "Gateway API proxy layer host terminated unexpectedly");
}
finally
{
    await Log.CloseAndFlushAsync();
}
public partial class Program;
