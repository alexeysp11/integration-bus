using FluentAssertions;
using Grpc.Core;
using IntegrationBus.Gateway.Api.Authentication;
using IntegrationBus.Identity.Contracts;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace IntegrationBus.Gateway.Api.Tests.Authentication;

/// <summary>
/// Exercises <see cref="TokenValidationMiddleware"/> against a stubbed gRPC validation client.
/// </summary>
public sealed class TokenValidationMiddlewareTests
{
    private readonly TokenValidation.TokenValidationClient _client = Substitute.For<TokenValidation.TokenValidationClient>();
    private bool _nextInvoked;

    private TokenValidationMiddleware CreateMiddleware(bool enabled = true)
    {
        RequestDelegate next = _ =>
        {
            _nextInvoked = true;
            return Task.CompletedTask;
        };

        return new TokenValidationMiddleware(
            next,
            NullLogger<TokenValidationMiddleware>.Instance,
            Options.Create(new IdentityOptions { Enabled = enabled }),
            _client);
    }

    private static DefaultHttpContext CreateContext(string path, string? authorization = null)
    {
        DefaultHttpContext context = new();
        context.Request.Path = path;
        context.Response.Body = new MemoryStream();
        if (authorization is not null)
        {
            context.Request.Headers.Authorization = authorization;
        }

        return context;
    }

    private void StubVerdict(ValidateTokenResponse response)
    {
        _client.ValidateTokenAsync(
                Arg.Any<ValidateTokenRequest>(),
                Arg.Any<Metadata>(),
                Arg.Any<DateTime?>(),
                Arg.Any<CancellationToken>())
            .Returns(new AsyncUnaryCall<ValidateTokenResponse>(
                Task.FromResult(response),
                Task.FromResult(new Metadata()),
                () => Status.DefaultSuccess,
                () => [],
                () => { }));
    }

    [Fact]
    public async Task InvokeAsync_WithoutAuthorizationHeader_ShouldReturn401WithoutCallingDownstream()
    {
        DefaultHttpContext context = CreateContext("/api/v1/ledger/transaction");

        await CreateMiddleware().InvokeAsync(context);

        context.Response.StatusCode.Should().Be(StatusCodes.Status401Unauthorized, "because anonymous calls are not allowed")
            .And.Be(401);
        context.Response.Headers.WWWAuthenticate.ToString().Should().StartWith("Bearer", "because RFC 6750 requires a Bearer challenge");
        _nextInvoked.Should().BeFalse("because the request must not reach the reverse proxy");
    }

    [Fact]
    public async Task InvokeAsync_WithInvalidToken_ShouldReturn401()
    {
        StubVerdict(new ValidateTokenResponse { IsValid = false, FailureReason = "expired" });
        DefaultHttpContext context = CreateContext("/api/v1/ledger/transaction", "Bearer bad.token.value");

        await CreateMiddleware().InvokeAsync(context);

        context.Response.StatusCode.Should().Be(StatusCodes.Status401Unauthorized, "because the identity service rejected the token");
        _nextInvoked.Should().BeFalse("because rejected requests must not be proxied");
    }

    [Fact]
    public async Task InvokeAsync_WithValidToken_ShouldForwardAndStampAuthenticatedSubject()
    {
        StubVerdict(new ValidateTokenResponse { IsValid = true, Subject = "user-42" });
        DefaultHttpContext context = CreateContext("/api/v1/ledger/transaction", "Bearer good.token.value");
        context.Request.Headers[TokenValidationMiddleware.SubjectHeader] = "spoofed-subject";

        await CreateMiddleware().InvokeAsync(context);

        _nextInvoked.Should().BeTrue("because a valid token lets the request through");
        context.Request.Headers[TokenValidationMiddleware.SubjectHeader].ToString()
            .Should().Be("user-42", "because the gateway overwrites any client-supplied subject header");
    }

    [Fact]
    public async Task InvokeAsync_WhenValidationServiceIsUnavailable_ShouldFailClosedWith503()
    {
        _client.ValidateTokenAsync(
                Arg.Any<ValidateTokenRequest>(),
                Arg.Any<Metadata>(),
                Arg.Any<DateTime?>(),
                Arg.Any<CancellationToken>())
            .Throws(new RpcException(new Status(StatusCode.Unavailable, "down")));
        DefaultHttpContext context = CreateContext("/api/v1/ledger/transaction", "Bearer good.token.value");

        await CreateMiddleware().InvokeAsync(context);

        context.Response.StatusCode.Should().Be(StatusCodes.Status503ServiceUnavailable, "because an unverifiable request must not be trusted");
        _nextInvoked.Should().BeFalse("because the gateway fails closed");
    }

    [Theory]
    [InlineData("/health")]
    [InlineData("/metrics")]
    public async Task InvokeAsync_ForOperationalEndpoints_ShouldSkipAuthentication(string path)
    {
        DefaultHttpContext context = CreateContext(path);

        await CreateMiddleware().InvokeAsync(context);

        _nextInvoked.Should().BeTrue("because probes and scrapers carry no token");
        _ = _client.DidNotReceive().ValidateTokenAsync(
            Arg.Any<ValidateTokenRequest>(),
            Arg.Any<Metadata>(),
            Arg.Any<DateTime?>(),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task InvokeAsync_WhenIdentityIsDisabled_ShouldPassThroughWithoutValidation()
    {
        DefaultHttpContext context = CreateContext("/api/v1/ledger/transaction");

        await CreateMiddleware(enabled: false).InvokeAsync(context);

        _nextInvoked.Should().BeTrue("because Identity:Enabled=false switches enforcement off");
    }
}
