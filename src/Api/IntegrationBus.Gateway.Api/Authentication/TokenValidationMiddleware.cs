using Grpc.Core;
using IntegrationBus.Identity.Contracts;
using Microsoft.Extensions.Options;

namespace IntegrationBus.Gateway.Api.Authentication;

/// <summary>
/// Rejects every request that does not carry a Keycloak-issued bearer token accepted by the identity service,
/// before the request reaches the reverse proxy.
/// </summary>
/// <remarks>
/// Failure semantics (401 versus fail-closed 503): <c>docs/api-gateway/README.md</c> (relative to the repository root).
/// </remarks>
public sealed class TokenValidationMiddleware(
    RequestDelegate next,
    ILogger<TokenValidationMiddleware> logger,
    IOptions<IdentityOptions> options,
    TokenValidation.TokenValidationClient client)
{
    /// <summary>
    /// The header carrying the authenticated subject to downstream services; always overwritten by the gateway.
    /// </summary>
    public const string SubjectHeader = "X-Authenticated-Subject";

    private static readonly string[] AnonymousPathPrefixes = ["/health", "/metrics", "/openapi"];

    /// <summary>
    /// Validates the bearer token of the current request and either forwards it or short-circuits with an error response.
    /// </summary>
    /// <param name="context">The current HTTP context.</param>
    public async Task InvokeAsync(HttpContext context)
    {
        IdentityOptions identity = options.Value;

        if (!identity.Enabled || IsAnonymousPath(context.Request.Path))
        {
            await next(context);
            return;
        }

        // A client must never be able to impersonate an authenticated subject
        context.Request.Headers.Remove(SubjectHeader);

        string? token = ExtractBearerToken(context.Request.Headers.Authorization.ToString());
        if (token is null)
        {
            await RejectAsync(context, StatusCodes.Status401Unauthorized, "A bearer token is required.");
            return;
        }

        ValidateTokenResponse response;
        try
        {
            response = await client.ValidateTokenAsync(
                new ValidateTokenRequest { Token = token },
                deadline: DateTime.UtcNow.AddSeconds(identity.TimeoutSeconds),
                cancellationToken: context.RequestAborted);
        }
        catch (RpcException ex)
        {
            // Without a verdict the request cannot be trusted, so the gateway fails closed
            logger.LogError(ex, "Token validation service is unavailable; rejecting the request.");
            await RejectAsync(context, StatusCodes.Status503ServiceUnavailable, "The authentication service is unavailable.");
            return;
        }

        if (!response.IsValid)
        {
            logger.LogWarning("Rejected request to {Path}: {Reason}", context.Request.Path, response.FailureReason);
            context.Response.Headers.WWWAuthenticate = "Bearer error=\"invalid_token\"";
            await RejectAsync(context, StatusCodes.Status401Unauthorized, "The bearer token is invalid or expired.");
            return;
        }

        context.Request.Headers[SubjectHeader] = response.Subject;
        await next(context);
    }

    private static bool IsAnonymousPath(PathString path) =>
        AnonymousPathPrefixes.Any(prefix => path.StartsWithSegments(prefix, StringComparison.OrdinalIgnoreCase));

    private static string? ExtractBearerToken(string authorizationHeader)
    {
        const string Scheme = "Bearer ";
        if (!authorizationHeader.StartsWith(Scheme, StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        string token = authorizationHeader[Scheme.Length..].Trim();
        return token.Length == 0 ? null : token;
    }

    private static async Task RejectAsync(HttpContext context, int statusCode, string message)
    {
        context.Response.StatusCode = statusCode;
        if (statusCode == StatusCodes.Status401Unauthorized && !context.Response.Headers.ContainsKey("WWW-Authenticate"))
        {
            context.Response.Headers.WWWAuthenticate = "Bearer";
        }

        await context.Response.WriteAsJsonAsync(new { status = statusCode, error = message });
    }
}
