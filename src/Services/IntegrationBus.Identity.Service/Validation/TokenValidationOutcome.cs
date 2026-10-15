namespace IntegrationBus.Identity.Service.Validation;

/// <summary>
/// Outcome of validating a bearer token.
/// </summary>
public sealed record TokenValidationOutcome
{
    /// <summary>
    /// Gets a value indicating whether the token is authentic, unexpired and addressed to this platform.
    /// </summary>
    public bool IsValid { get; init; }

    /// <summary>
    /// Gets the <c>sub</c> claim of a valid token.
    /// </summary>
    public string Subject { get; init; } = string.Empty;

    /// <summary>
    /// Gets the rejection reason for an invalid token.
    /// </summary>
    public string FailureReason { get; init; } = string.Empty;

    /// <summary>
    /// Gets the realm roles carried by a valid token.
    /// </summary>
    public IReadOnlyList<string> Roles { get; init; } = [];
}
