namespace IntegrationBus.Compliance.Service.Rules;

/// <summary>
/// Outcome of evaluating a <see cref="ComplianceRuleInput"/> against the declarative JSON rule workflow.
/// </summary>
public sealed record ComplianceEvaluationResult
{
    public required bool IsCompliant { get; init; }

    public string? FailureReason { get; init; }
}
