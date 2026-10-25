namespace IntegrationBus.Gateway.Api.Authentication;

/// <summary>
/// Binds the <c>Identity</c> configuration section that controls gateway-level authentication.
/// </summary>
public sealed class IdentityOptions
{
    /// <summary>
    /// Gets or sets a value indicating whether bearer tokens are enforced. Enabled by default.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Gets or sets the address of the gRPC token validation service; required when <see cref="Enabled"/> is true and supplied through configuration.
    /// </summary>
    public string? GrpcAddress { get; set; }

    /// <summary>
    /// Gets or sets the deadline, in seconds, for one validation call.
    /// </summary>
    public int TimeoutSeconds { get; set; } = 3;
}
