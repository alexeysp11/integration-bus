namespace IntegrationBus.Identity.Service.Configurations;

/// <summary>
/// Binds the <c>Keycloak</c> configuration section that describes the trusted identity provider.
/// </summary>
public sealed class KeycloakOptions
{
    /// <summary>
    /// Gets or sets the realm issuer URL (for example <c>http://keycloak:8080/realms/integration-bus</c>); the OIDC
    /// discovery document is resolved from <c>{Authority}/.well-known/openid-configuration</c>.
    /// </summary>
    public string Authority { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the expected token audience. An empty value disables audience validation.
    /// </summary>
    public string Audience { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets a value indicating whether the discovery endpoint must use HTTPS. Disabled for local development only.
    /// </summary>
    public bool RequireHttpsMetadata { get; set; } = true;
}
