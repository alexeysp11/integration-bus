using FluentValidation;
using FluentValidation.AspNetCore;
using IntegrationBus.Contracts.Http;
using IntegrationBus.Processing.Api.Validation;

namespace IntegrationBus.Processing.Api.Extensions;

/// <summary>
/// Provides centralized and explicit dependency injection registration boundaries for application contract validators.
/// </summary>
public static class ValidationDependencyInjectionExtensions
{
    /// <summary>
    /// Explicitly registers all inbound API contract validators with optimized lifetimes into the DI container.
    /// </summary>
    /// <param name="services">The core target service collection container.</param>
    /// <returns>The modified service collection to preserve fluent invocation chains.</returns>
    public static IServiceCollection AddApiValidators(this IServiceCollection services)
    {
        services.AddSingleton<IValidator<TopUpAccountRequest>, TopUpAccountRequestValidator>();
        services.AddSingleton<IValidator<StartTransactionRequest>, StartTransactionRequestValidator>();

        services.AddFluentValidationAutoValidation();

        return services;
    }
}
