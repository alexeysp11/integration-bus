namespace IntegrationBus.Compliance.Service.Rules;

/// <summary>
/// Binds the <c>ComplianceRules</c> configuration section, resolving where the declarative JSON rule workflow lives on disk.
/// </summary>
public sealed class ComplianceRulesOptions
{
    /// <summary>
    /// Path to the JSON workflow file, relative to the application base directory unless rooted.
    /// </summary>
    public required string RulesFilePath { get; init; }
}
