using IntegrationBus.Contracts.Http;
using Microsoft.AspNetCore.Mvc.ModelBinding;
using Microsoft.AspNetCore.Mvc;

namespace IntegrationBus.Processing.Api.Validation;

/// <summary>
/// Provides manufacturing boundaries to convert system model state errors into structured, flat production DTO payloads.
/// </summary>
public static class ValidationErrorResponseFactory
{
    /// <summary>
    /// Transforms the native MVC ModelState errors into a strongly-typed <see cref="BadRequestObjectResult"/> 
    /// containing a flat <see cref="ValidationErrorResponse"/>.
    /// </summary>
    /// <param name="modelState">The current execution pipeline model state dictionary container.</param>
    /// <returns>A formatted HTTP 400 Bad Request action result metadata object.</returns>
    public static BadRequestObjectResult Create(ModelStateDictionary modelState)
    {
        string firstError = modelState.Values
            .SelectMany(v => v.Errors)
            .Select(e => e.ErrorMessage)
            .FirstOrDefault(msg => !string.IsNullOrWhiteSpace(msg))
            ?? "One or more validation errors occurred.";

        ValidationErrorResponse errorPayload = new()
        {
            Error = firstError
        };

        return new BadRequestObjectResult(errorPayload);
    }
}
