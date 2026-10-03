namespace IntegrationBus.Compliance.Service.Rules;

/// <summary>
/// Evaluates a transaction against the declarative, JSON-configured compliance rule workflow.
/// </summary>
public interface IComplianceRulesEvaluator
{
    /// <summary>
    /// Runs every rule in the <c>TransactionComplianceWorkflow</c> against <paramref name="input"/>.
    /// </summary>
    /// <param name="input">The flattened transaction facts to evaluate.</param>
    /// <param name="cancellationToken">Propagates cancellation from the consuming message context.</param>
    /// <returns>An aggregated compliance verdict; on failure, <see cref="ComplianceEvaluationResult.FailureReason"/> carries the first violated rule's message.</returns>
    Task<ComplianceEvaluationResult> EvaluateAsync(ComplianceRuleInput input, CancellationToken cancellationToken);
}
