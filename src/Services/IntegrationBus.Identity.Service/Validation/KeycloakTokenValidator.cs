using System.Text.Json;
using IntegrationBus.Identity.Service.Configurations;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Protocols;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;
using Microsoft.IdentityModel.Tokens;

namespace IntegrationBus.Identity.Service.Validation;

/// <summary>
/// Validates Keycloak-issued JWTs against the signing keys and issuer published in the realm's OIDC discovery document.
/// </summary>
/// <remarks>
/// Design and failure semantics: <c>docs/api-gateway/README.md</c> (relative to the repository root).
/// </remarks>
public sealed class KeycloakTokenValidator(
    ILogger<KeycloakTokenValidator> logger,
    IOptions<KeycloakOptions> options,
    IConfigurationManager<OpenIdConnectConfiguration> configurationManager) : ITokenValidator
{
    private static readonly JsonWebTokenHandler TokenHandler = new();

    /// <inheritdoc />
    public async Task<TokenValidationOutcome> ValidateAsync(string token, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(token))
        {
            return Reject("The token is empty.");
        }

        OpenIdConnectConfiguration configuration = await configurationManager.GetConfigurationAsync(cancellationToken);

        TokenValidationParameters parameters = new()
        {
            ValidIssuer = configuration.Issuer,
            IssuerSigningKeys = configuration.SigningKeys,
            ValidateIssuer = true,
            ValidateIssuerSigningKey = true,
            ValidateLifetime = true,
            ValidateAudience = !string.IsNullOrWhiteSpace(options.Value.Audience),
            ValidAudience = options.Value.Audience,
            ClockSkew = TimeSpan.FromSeconds(30)
        };

        TokenValidationResult result = await TokenHandler.ValidateTokenAsync(token, parameters);

        if (!result.IsValid)
        {
            logger.LogWarning("Bearer token rejected: {Reason}", result.Exception?.Message);
            return Reject(result.Exception?.Message ?? "The token failed validation.");
        }

        return new TokenValidationOutcome
        {
            IsValid = true,
            Subject = result.ClaimsIdentity.FindFirst(JwtRegisteredClaimNames.Sub)?.Value ?? string.Empty,
            Roles = ExtractRealmRoles(result.ClaimsIdentity.FindFirst("realm_access")?.Value)
        };
    }

    private static TokenValidationOutcome Reject(string reason) => new() { IsValid = false, FailureReason = reason };

    private static List<string> ExtractRealmRoles(string? realmAccessJson)
    {
        if (string.IsNullOrWhiteSpace(realmAccessJson))
        {
            return [];
        }

        try
        {
            using JsonDocument document = JsonDocument.Parse(realmAccessJson);
            if (document.RootElement.TryGetProperty("roles", out JsonElement roles) && roles.ValueKind == JsonValueKind.Array)
            {
                return roles.EnumerateArray()
                    .Select(role => role.GetString() ?? string.Empty)
                    .Where(role => role.Length > 0)
                    .ToList();
            }
        }
        catch (JsonException)
        {
            // A malformed realm_access claim simply yields no roles
        }

        return [];
    }
}
