using System.Security.Cryptography;
using FluentAssertions;
using IntegrationBus.Identity.Service.Configurations;
using IntegrationBus.Identity.Service.Validation;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Protocols;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;
using Microsoft.IdentityModel.Tokens;
using NSubstitute;

namespace IntegrationBus.Identity.Service.Tests.Validation;

/// <summary>
/// Exercises <see cref="KeycloakTokenValidator"/> with locally signed tokens and a stubbed OIDC discovery document.
/// </summary>
public sealed class KeycloakTokenValidatorTests : IDisposable
{
    private const string Issuer = "http://keycloak.test/realms/integration-bus";
    private const string Audience = "integration-bus-api";

    private readonly RSA _trustedKey = RSA.Create(2048);
    private readonly RSA _foreignKey = RSA.Create(2048);

    public void Dispose()
    {
        _trustedKey.Dispose();
        _foreignKey.Dispose();
    }

    private KeycloakTokenValidator CreateValidator(string audience = Audience)
    {
        OpenIdConnectConfiguration configuration = new() { Issuer = Issuer };
        configuration.SigningKeys.Add(new RsaSecurityKey(_trustedKey) { KeyId = "trusted" });

        IConfigurationManager<OpenIdConnectConfiguration> manager = Substitute.For<IConfigurationManager<OpenIdConnectConfiguration>>();
        manager.GetConfigurationAsync(Arg.Any<CancellationToken>()).Returns(configuration);

        return new KeycloakTokenValidator(
            NullLogger<KeycloakTokenValidator>.Instance,
            Options.Create(new KeycloakOptions { Authority = Issuer, Audience = audience }),
            manager);
    }

    private static string CreateToken(
        RSA signingKey,
        string issuer = Issuer,
        string audience = Audience,
        DateTime? expires = null)
    {
        DateTime now = DateTime.UtcNow;
        SecurityTokenDescriptor descriptor = new()
        {
            Issuer = issuer,
            Audience = audience,
            NotBefore = now.AddMinutes(-10),
            IssuedAt = now.AddMinutes(-10),
            Expires = expires ?? now.AddMinutes(5),
            Claims = new Dictionary<string, object>
            {
                ["sub"] = "user-42",
                ["realm_access"] = "{\"roles\":[\"ledger-writer\",\"auditor\"]}"
            },
            SigningCredentials = new SigningCredentials(new RsaSecurityKey(signingKey) { KeyId = "trusted" }, SecurityAlgorithms.RsaSha256)
        };

        return new JsonWebTokenHandler().CreateToken(descriptor);
    }

    [Fact]
    public async Task ValidateAsync_WithValidToken_ShouldAcceptAndExposeSubjectAndRoles()
    {
        KeycloakTokenValidator validator = CreateValidator();

        TokenValidationOutcome outcome = await validator.ValidateAsync(CreateToken(_trustedKey), CancellationToken.None);

        outcome.IsValid.Should().BeTrue("because the token is signed by the trusted realm key and is unexpired");
        outcome.Subject.Should().Be("user-42", "because the sub claim identifies the caller");
        outcome.Roles.Should().BeEquivalentTo(["ledger-writer", "auditor"], "because realm roles are carried in the realm_access claim");
    }

    [Fact]
    public async Task ValidateAsync_WithExpiredToken_ShouldReject()
    {
        KeycloakTokenValidator validator = CreateValidator();
        string token = CreateToken(_trustedKey, expires: DateTime.UtcNow.AddMinutes(-5));

        TokenValidationOutcome outcome = await validator.ValidateAsync(token, CancellationToken.None);

        outcome.IsValid.Should().BeFalse("because an expired token must never authenticate a request");
        outcome.FailureReason.Should().NotBeEmpty("because the caller needs a rejection reason");
    }

    [Fact]
    public async Task ValidateAsync_WithForeignIssuer_ShouldReject()
    {
        KeycloakTokenValidator validator = CreateValidator();
        string token = CreateToken(_trustedKey, issuer: "http://evil.test/realms/other");

        TokenValidationOutcome outcome = await validator.ValidateAsync(token, CancellationToken.None);

        outcome.IsValid.Should().BeFalse("because tokens from another issuer are not trusted");
    }

    [Fact]
    public async Task ValidateAsync_WithTokenSignedByUnknownKey_ShouldReject()
    {
        KeycloakTokenValidator validator = CreateValidator();
        string token = CreateToken(_foreignKey);

        TokenValidationOutcome outcome = await validator.ValidateAsync(token, CancellationToken.None);

        outcome.IsValid.Should().BeFalse("because a signature that does not match the published realm keys is a forgery");
    }

    [Fact]
    public async Task ValidateAsync_WithWrongAudience_ShouldReject()
    {
        KeycloakTokenValidator validator = CreateValidator();
        string token = CreateToken(_trustedKey, audience: "some-other-api");

        TokenValidationOutcome outcome = await validator.ValidateAsync(token, CancellationToken.None);

        outcome.IsValid.Should().BeFalse("because the token is addressed to a different resource server");
    }

    [Fact]
    public async Task ValidateAsync_WithEmptyConfiguredAudience_ShouldSkipAudienceCheck()
    {
        KeycloakTokenValidator validator = CreateValidator(audience: string.Empty);
        string token = CreateToken(_trustedKey, audience: "any-audience");

        TokenValidationOutcome outcome = await validator.ValidateAsync(token, CancellationToken.None);

        outcome.IsValid.Should().BeTrue("because an empty Keycloak:Audience disables audience validation");
    }

    [Theory]
    [InlineData("")]
    [InlineData("not-a-jwt")]
    public async Task ValidateAsync_WithMalformedToken_ShouldRejectWithoutThrowing(string token)
    {
        KeycloakTokenValidator validator = CreateValidator();

        TokenValidationOutcome outcome = await validator.ValidateAsync(token, CancellationToken.None);

        outcome.IsValid.Should().BeFalse("because malformed input is a rejection, not a server error");
    }
}
