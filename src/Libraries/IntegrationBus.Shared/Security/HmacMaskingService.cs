using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace IntegrationBus.Shared.Security;

/// <summary>
/// Reads the masking secret exclusively from the <c>MASKING_HMAC_SALT</c> process environment variable and applies
/// HMACSHA256 over UTF-8 encoded input to produce a deterministic, non-reversible hexadecimal digest.
/// </summary>
public sealed class HmacMaskingService : IHmacMaskingService
{
    /// <summary>
    /// Sentinel returned for <see langword="null"/> input; deliberately distinct from a 64-character hex digest
    /// so a masked security-topic payload can be told apart from a genuinely hashed value at a glance.
    /// </summary>
    public const string NullValueToken = "<null>";

    private readonly byte[] _saltBytes;

    public HmacMaskingService()
    {
        string? salt = Environment.GetEnvironmentVariable("MASKING_HMAC_SALT");

        if (string.IsNullOrEmpty(salt))
        {
            throw new InvalidOperationException(
                "The MASKING_HMAC_SALT environment variable must be set to a non-empty secret before the data masking pipeline can run.");
        }

        _saltBytes = Encoding.UTF8.GetBytes(salt);
    }

    /// <inheritdoc />
    public string Mask(string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return NullValueToken;
        }

        using HMACSHA256 hmac = new(_saltBytes);
        byte[] digest = hmac.ComputeHash(Encoding.UTF8.GetBytes(value));

        return Convert.ToHexString(digest);
    }

    /// <inheritdoc />
    public string MaskValue(object? value)
    {
        return value switch
        {
            null => NullValueToken,
            string text => Mask(text),
            IFormattable formattable => Mask(formattable.ToString(null, CultureInfo.InvariantCulture)),
            _ => Mask(value.ToString())
        };
    }
}
