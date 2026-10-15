using Grpc.Core;
using IntegrationBus.Identity.Contracts;
using IntegrationBus.Identity.Service.Validation;

namespace IntegrationBus.Identity.Service.Services;

/// <summary>
/// Exposes <see cref="ITokenValidator"/> over gRPC so the API gateway can authenticate every inbound request.
/// </summary>
public sealed class TokenValidationGrpcService(ITokenValidator validator) : TokenValidation.TokenValidationBase
{
    /// <summary>
    /// Validates the supplied bearer token and reports the outcome; a rejected token is a normal response, not a gRPC error.
    /// </summary>
    /// <param name="request">The request carrying the raw JWT.</param>
    /// <param name="context">The server call context.</param>
    /// <returns>The validation verdict, subject and roles.</returns>
    public override async Task<ValidateTokenResponse> ValidateToken(ValidateTokenRequest request, ServerCallContext context)
    {
        TokenValidationOutcome outcome = await validator.ValidateAsync(request.Token, context.CancellationToken);

        ValidateTokenResponse response = new()
        {
            IsValid = outcome.IsValid,
            Subject = outcome.Subject,
            FailureReason = outcome.FailureReason
        };
        response.Roles.AddRange(outcome.Roles);

        return response;
    }
}
