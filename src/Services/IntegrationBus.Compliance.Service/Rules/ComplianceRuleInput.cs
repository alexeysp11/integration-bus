namespace IntegrationBus.Compliance.Service.Rules;

/// <summary>
/// Flattened, rule-engine-facing projection of a <see cref="IntegrationBus.Compliance.Contracts.Messages.Commands.CheckComplianceLimits"/> command.
/// </summary>
public sealed record ComplianceRuleInput
{
    public required Guid TransactionId { get; init; }

    public required Guid SourceAccountId { get; init; }

    public required Guid TargetAccountId { get; init; }

    public required decimal Amount { get; init; }

    public required int Currency { get; init; }
}
