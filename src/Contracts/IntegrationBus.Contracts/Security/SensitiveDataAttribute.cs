namespace IntegrationBus.Contracts.Security;

/// <summary>
/// Marks a message contract property as confidential (account identifiers, balances) so the infrastructure-level
/// masking pipeline in <c>IntegrationBus.Shared</c> knows to obfuscate it with a salted HMAC hash before the value
/// is ever mirrored into a Kafka security topic.
/// </summary>
[AttributeUsage(AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
public sealed class SensitiveDataAttribute : Attribute;
