using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

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

    /// <summary>
    /// Configures the OpenTelemetry resource attributes shared by metrics, traces, and log enrichment
    /// so every telemetry signal emitted by this service reports under the same logical service name in Grafana/Jaeger.
    /// </summary>
    /// <param name="services">The target service collection container instance.</param>
    /// <param name="serviceName">The logical service name reported to Prometheus, Jaeger, and Loki.</param>
    /// <returns>The mutated service collection instance to facilitate fluent configuration chaining.</returns>
    public static IServiceCollection AddTelemetryResource(this IServiceCollection services, string serviceName)
    {
        services.AddOpenTelemetry()
            .ConfigureResource(resource => resource.AddService(
                serviceName: serviceName,
                serviceInstanceId: Environment.MachineName));

        return services;
    }

    /// <summary>
    /// Enables end-to-end distributed tracing across MassTransit/Kafka message flows, inbound and outbound HTTP calls,
    /// and Entity Framework Core database commands, exporting every span via OTLP to the centralized Jaeger collector.
    /// </summary>
    /// <remarks>
    /// The OTLP exporter target is resolved from the standard <c>OTEL_EXPORTER_OTLP_ENDPOINT</c> environment variable.
    /// </remarks>
    /// <param name="services">The target service collection container instance.</param>
    /// <returns>The mutated service collection instance to facilitate fluent configuration chaining.</returns>
    public static IServiceCollection AddDistributedTracing(this IServiceCollection services)
    {
        services.AddOpenTelemetry()
            .WithTracing(tracing => tracing
                .AddSource("MassTransit")
                .AddAspNetCoreInstrumentation()
                .AddHttpClientInstrumentation()
                .AddEntityFrameworkCoreInstrumentation()
                .AddOtlpExporter());

        return services;
    }
}
