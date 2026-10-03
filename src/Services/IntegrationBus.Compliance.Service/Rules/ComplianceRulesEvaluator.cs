using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Options;
using RulesEngine.Models;

namespace IntegrationBus.Compliance.Service.Rules;

/// <summary>
/// Loads the <c>TransactionComplianceWorkflow</c> definition from a local JSON file once at startup and evaluates
/// every incoming transaction against it via <see cref="global::RulesEngine.RulesEngine"/>, replacing what used to be
/// a hardcoded always-pass compliance check with an externally configurable, auditable rule set.
/// </summary>
public sealed class ComplianceRulesEvaluator : IComplianceRulesEvaluator
{
    private const string WorkflowName = "TransactionComplianceWorkflow";

    private static readonly JsonSerializerOptions RulesFileSerializerOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        Converters = { new JsonStringEnumConverter() }
    };

    private readonly global::RulesEngine.RulesEngine _rulesEngine;

    public ComplianceRulesEvaluator(IOptions<ComplianceRulesOptions> options, ILogger<ComplianceRulesEvaluator> logger)
    {
        string rulesFilePath = Path.IsPathRooted(options.Value.RulesFilePath)
            ? options.Value.RulesFilePath
            : Path.Combine(AppContext.BaseDirectory, options.Value.RulesFilePath);

        string rulesJson = File.ReadAllText(rulesFilePath);

        Workflow[] workflows = JsonSerializer.Deserialize<List<Workflow>>(rulesJson, RulesFileSerializerOptions)?.ToArray()
            ?? throw new InvalidOperationException($"Compliance rules file at '{rulesFilePath}' did not contain a valid workflow definition.");

        _rulesEngine = new global::RulesEngine.RulesEngine(workflows);

        logger.LogInformation(
            "Loaded compliance rule workflow '{WorkflowName}' with {RuleCount} rule(s) from {RulesFilePath}",
            WorkflowName,
            workflows.FirstOrDefault(w => w.WorkflowName == WorkflowName)?.Rules?.Count() ?? 0,
            rulesFilePath);
    }

    /// <inheritdoc />
    public async Task<ComplianceEvaluationResult> EvaluateAsync(ComplianceRuleInput input, CancellationToken cancellationToken)
    {
        RuleParameter parameter = new("input", input);

        List<RuleResultTree> results = await _rulesEngine.ExecuteAllRulesAsync(WorkflowName, [parameter]);

        RuleResultTree? violatedRule = results.FirstOrDefault(result => !result.IsSuccess);

        return violatedRule is null
            ? new ComplianceEvaluationResult { IsCompliant = true }
            : new ComplianceEvaluationResult
            {
                IsCompliant = false,
                FailureReason = violatedRule.Rule.ErrorMessage ?? $"Compliance rule '{violatedRule.Rule.RuleName}' was violated."
            };
    }
}
