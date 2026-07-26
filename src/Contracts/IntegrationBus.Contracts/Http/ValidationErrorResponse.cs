namespace IntegrationBus.Contracts.Http;

/// <summary>
/// Represents a flat, strongly-typed HTTP API error boundary contract payload.
/// </summary>
public sealed record ValidationErrorResponse
{
    /// <summary>
    /// Gets the concrete, human-readable reason detailing the first encountered validation constraint failure.
    /// </summary>
    /// <example>Top-up amount must be strictly greater than zero.</example>
    public string? Error { get; init; }
}
