namespace IntegrationBus.Shared.Security;

/// <summary>
/// Deterministically obfuscates confidential values (account identifiers, balances) using a salted HMACSHA256 digest,
/// so the same raw input always maps to the same masked token without the raw value ever leaving the process.
/// </summary>
public interface IHmacMaskingService
{
    /// <summary>
    /// Masks a raw string value. Never throws for any input, including <see langword="null"/> or empty strings.
    /// </summary>
    string Mask(string? value);

    /// <summary>
    /// Masks an arbitrarily typed value (<see cref="Guid"/>, <see cref="decimal"/>, boxed primitives, or any object)
    /// by falling back to its <see cref="object.ToString"/> representation. Never throws.
    /// </summary>
    string MaskValue(object? value);
}
