using FluentAssertions;
using IntegrationBus.Shared.Security;

namespace IntegrationBus.Shared.Tests.Security;

/// <summary>
/// All tests in this class share process-wide mutable state (<c>MASKING_HMAC_SALT</c>), so xUnit's default
/// same-class sequential execution is relied upon rather than explicit locking.
/// </summary>
public sealed class HmacMaskingServiceTests : IDisposable
{
    private const string EnvironmentVariableName = "MASKING_HMAC_SALT";
    private readonly string? _originalSalt = Environment.GetEnvironmentVariable(EnvironmentVariableName);

    public void Dispose() => Environment.SetEnvironmentVariable(EnvironmentVariableName, _originalSalt);

    [Fact]
    public void Constructor_WithEmptySalt_ShouldThrowInvalidOperationException()
    {
        Environment.SetEnvironmentVariable(EnvironmentVariableName, string.Empty);

        Action act = () => _ = new HmacMaskingService();

        act.Should().Throw<InvalidOperationException>("because the pipeline must never mask sensitive data with an empty, effectively-disabled secret")
            .WithMessage("*MASKING_HMAC_SALT*");
    }

    [Fact]
    public void Constructor_WithMissingSalt_ShouldThrowInvalidOperationException()
    {
        Environment.SetEnvironmentVariable(EnvironmentVariableName, null);

        Action act = () => _ = new HmacMaskingService();

        act.Should().Throw<InvalidOperationException>("because a completely unset salt is exactly as unsafe as an empty one");
    }

    [Fact]
    public void Mask_WithSameInputAndSalt_ShouldBeDeterministic()
    {
        Environment.SetEnvironmentVariable(EnvironmentVariableName, "test-salt-1");
        HmacMaskingService sut = new();

        string first = sut.Mask("a2222222-3333-4444-5555-999999999999");
        string second = sut.Mask("a2222222-3333-4444-5555-999999999999");

        first.Should()
            .Be(second, "because masking the same raw value under the same salt must always yield the same digest")
            .And.MatchRegex("^[0-9A-F]{64}$", "because HMACSHA256 produces a 32-byte digest rendered as 64 uppercase hex characters");
    }

    [Fact]
    public void Mask_WithDifferentSalts_ShouldProduceDifferentDigests()
    {
        const string rawValue = "a2222222-3333-4444-5555-999999999999";

        Environment.SetEnvironmentVariable(EnvironmentVariableName, "salt-a");
        string maskedWithSaltA = new HmacMaskingService().Mask(rawValue);

        Environment.SetEnvironmentVariable(EnvironmentVariableName, "salt-b");
        string maskedWithSaltB = new HmacMaskingService().Mask(rawValue);

        maskedWithSaltA.Should().NotBe(maskedWithSaltB, "because the salt must be mixed into the digest, not just appended cosmetically");
    }

    [Fact]
    public void Mask_WithNullOrEmptyValue_ShouldReturnNullToken()
    {
        Environment.SetEnvironmentVariable(EnvironmentVariableName, "test-salt-1");
        HmacMaskingService sut = new();

        sut.Mask(null).Should().Be(HmacMaskingService.NullValueToken, "because masking null must never throw or silently hash an empty string as if it carried data");
        sut.Mask(string.Empty).Should().Be(HmacMaskingService.NullValueToken, "because an empty string carries no information either");
    }

    [Fact]
    public void MaskValue_WithGuid_ShouldMatchMaskingItsStringRepresentation()
    {
        Environment.SetEnvironmentVariable(EnvironmentVariableName, "test-salt-1");
        HmacMaskingService sut = new();
        Guid accountId = Guid.Parse("a2222222-3333-4444-5555-999999999999");

        sut.MaskValue(accountId).Should().Be(sut.Mask(accountId.ToString()),
            "because a Guid must be masked using its canonical invariant string form for reproducibility");
    }

    [Fact]
    public void MaskValue_WithDecimalAmount_ShouldNotThrowAndShouldBeDeterministic()
    {
        Environment.SetEnvironmentVariable(EnvironmentVariableName, "test-salt-1");
        HmacMaskingService sut = new();

        string first = sut.MaskValue(1500.75m);
        string second = sut.MaskValue(1500.75m);

        first.Should().Be(second, "because decimal amounts are a core masked field and must hash deterministically like every other type");
    }

    [Fact]
    public void MaskValue_WithNull_ShouldReturnNullToken()
    {
        Environment.SetEnvironmentVariable(EnvironmentVariableName, "test-salt-1");
        HmacMaskingService sut = new();

        sut.MaskValue(null).Should().Be(HmacMaskingService.NullValueToken, "because reflection over optional message fields will sometimes yield null");
    }

    [Fact]
    public void MaskValue_WithUnexpectedObjectType_ShouldFallBackToToStringWithoutThrowing()
    {
        Environment.SetEnvironmentVariable(EnvironmentVariableName, "test-salt-1");
        HmacMaskingService sut = new();
        object arbitraryPayload = new { Nested = "value" };

        Action act = () => sut.MaskValue(arbitraryPayload);

        act.Should().NotThrow("because the masking pipeline must stay resilient even when a contract exposes an unexpected, non-primitive property type");
    }
}
