using IntegrationBus.Identity.Service.Configurations;
using IntegrationBus.Identity.Service.Services;
using IntegrationBus.Identity.Service.Validation;
using IntegrationBus.Shared.Extensions;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Protocols;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;
using Serilog;

try
{
    WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

    // Bootstrap logging layers immediately to track container structural allocation phases
    Log.Logger = new LoggerConfiguration()
        .ReadFrom.Configuration(builder.Configuration)
        .CreateLogger();

    builder.Logging.ClearProviders();
    builder.Logging.AddSerilog();

    builder.Services
        .AddTelemetryResource("integration-bus-identity-service")
        .AddCoreMetrics()
        .AddHttpMetrics()
        .AddDistributedTracing();

    builder.Services.Configure<KeycloakOptions>(builder.Configuration.GetSection("Keycloak"));

    // The configuration manager caches the discovery document and refreshes signing keys on rotation
    builder.Services.AddSingleton<IConfigurationManager<OpenIdConnectConfiguration>>(sp =>
    {
        KeycloakOptions keycloak = sp.GetRequiredService<IOptions<KeycloakOptions>>().Value;
        if (string.IsNullOrWhiteSpace(keycloak.Authority))
        {
            throw new InvalidOperationException("Keycloak:Authority is not specified");
        }

        string metadataAddress = $"{keycloak.Authority.TrimEnd('/')}/.well-known/openid-configuration";
        return new ConfigurationManager<OpenIdConnectConfiguration>(
            metadataAddress,
            new OpenIdConnectConfigurationRetriever(),
            new HttpDocumentRetriever { RequireHttps = keycloak.RequireHttpsMetadata });
    });

    builder.Services.AddSingleton<ITokenValidator, KeycloakTokenValidator>();
    builder.Services.AddGrpc();
    builder.Services.AddHealthChecks();

    WebApplication app = builder.Build();

    app.MapGrpcService<TokenValidationGrpcService>();
    app.MapHealthChecks("/health");
    app.UseMetricsScraping();

    await app.RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "Identity service host terminated unexpectedly");
}
finally
{
    await Log.CloseAndFlushAsync();
}
