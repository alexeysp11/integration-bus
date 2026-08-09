using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using OpenTelemetry.Metrics;

namespace IntegrationBus.Shared.Extensions;

/// <summary>
/// Provides isolated, production-grade infrastructure extensions for OpenTelemetry metrics collection.
/// </summary>
public static class TelemetryExtensions
{
    /// <summary>
    /// Initializes core OpenTelemetry metrics engine with standard .NET runtime instrumentation.
    /// </summary>
    /// <param name="services">The target service collection container instance.</param>
    /// <returns>The mutated service collection instance to facilitate fluent configuration chaining.</returns>
    public static IServiceCollection AddCoreMetrics(this IServiceCollection services)
    {
        services.AddOpenTelemetry()
            .WithMetrics(metrics => metrics
                .AddRuntimeInstrumentation()
                .AddPrometheusExporter());

        return services;
    }

    /// <summary>
    /// Appends ASP.NET Core web request tracking instrumentation to the existing OpenTelemetry metrics pipeline.
    /// </summary>
    /// <param name="services">The target service collection container instance.</param>
    /// <returns>The mutated service collection instance to facilitate fluent configuration chaining.</returns>
    public static IServiceCollection AddHttpMetrics(this IServiceCollection services)
    {
        services.ConfigureOpenTelemetryMeterProvider(metrics => metrics
            .AddAspNetCoreInstrumentation());

        return services;
    }

    /// <summary>
    /// Appends MassTransit asynchronous message execution tracking instrumentation to the existing OpenTelemetry metrics pipeline.
    /// </summary>
    /// <param name="services">The target service collection container instance.</param>
    /// <returns>The mutated service collection instance to facilitate fluent configuration chaining.</returns>
    public static IServiceCollection AddMassTransitMetrics(this IServiceCollection services)
    {
        services.ConfigureOpenTelemetryMeterProvider(metrics => metrics
            .AddMeter("MassTransit"));

        return services;
    }

    /// <summary>
    /// Maps the immutable standard Prometheus endpoint pattern onto the application middleware routing pipeline.
    /// </summary>
    /// <param name="app">The active running web application execution pipeline instance.</param>
    /// <returns>The mutated web application instance to facilitate fluent configuration chaining.</returns>
    public static WebApplication UseMetricsScraping(this WebApplication app)
    {
        app.MapPrometheusScrapingEndpoint();
        return app;
    }
}
