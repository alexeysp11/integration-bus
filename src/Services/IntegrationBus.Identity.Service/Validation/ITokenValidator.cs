namespace IntegrationBus.Identity.Service.Validation;

/// <summary>
/// Validates bearer tokens issued by the trusted identity provider.
/// </summary>
public interface ITokenValidator
{
    /// <summary>
    /// Verifies the signature, issuer, lifetime and audience of a JWT.
    /// </summary>
    /// <param name="token">The raw JWT without the <c>Bearer</c> scheme prefix.</param>
    /// <param name="cancellationToken">Propagates notification that the operation should be canceled.</param>
    /// <returns>The validation outcome; never throws for a malformed or rejected token.</returns>
    Task<TokenValidationOutcome> ValidateAsync(string token, CancellationToken cancellationToken);
}
