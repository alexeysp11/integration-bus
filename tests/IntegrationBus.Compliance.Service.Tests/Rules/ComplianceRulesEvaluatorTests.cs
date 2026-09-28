using FluentAssertions;
using IntegrationBus.Compliance.Service.Rules;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace IntegrationBus.Compliance.Service.Tests.Rules;

/// <summary>
/// Exercises <see cref="ComplianceRulesEvaluator"/> against the actual production <c>compliance-rules.json</c>
/// workflow (linked into this project's output), so a regression in the declarative rule file itself is caught here
/// rather than only in a hand-maintained copy.
/// </summary>
public sealed class ComplianceRulesEvaluatorTests
{
    private static ComplianceRulesEvaluator CreateEvaluator()
    {
        IOptions<ComplianceRulesOptions> options = Options.Create(new ComplianceRulesOptions
        {
            RulesFilePath = "Rules/compliance-rules.json"
        });

        return new ComplianceRulesEvaluator(options, NullLogger<ComplianceRulesEvaluator>.Instance);
    }

    private static ComplianceRuleInput CreateBaselineCompliantInput() => new()
    {
        TransactionId = Guid.NewGuid(),
        SourceAccountId = Guid.NewGuid(),
        TargetAccountId = Guid.NewGuid(),
        Amount = 1500.00m,
        Currency = 1
    };

    [Fact]
    public async Task EvaluateAsync_WithFullyCompliantTransaction_ShouldReturnCompliant()
    {
        ComplianceRulesEvaluator sut = CreateEvaluator();

        ComplianceEvaluationResult result = await sut.EvaluateAsync(CreateBaselineCompliantInput(), CancellationToken.None);

        result.IsCompliant.Should().BeTrue("because the input violates none of the declarative rules in the workflow");
        result.FailureReason.Should().BeNull("because a compliant verdict must not carry a stale or misleading rejection reason");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-500)]
    public async Task EvaluateAsync_WithNonPositiveAmount_ShouldFailPositiveTransactionAmountRule(decimal amount)
    {
        ComplianceRulesEvaluator sut = CreateEvaluator();
        ComplianceRuleInput input = CreateBaselineCompliantInput() with { Amount = amount };

        ComplianceEvaluationResult result = await sut.EvaluateAsync(input, CancellationToken.None);

        result.IsCompliant.Should().BeFalse("because zero and negative amounts must never clear compliance");
        result.FailureReason.Should().Contain("greater than zero", "because the PositiveTransactionAmount rule's configured message must surface verbatim");
    }

    [Fact]
    public async Task EvaluateAsync_WithAmountAboveMaxThreshold_ShouldFailMaxSingleTransactionAmountRule()
    {
        ComplianceRulesEvaluator sut = CreateEvaluator();
        ComplianceRuleInput input = CreateBaselineCompliantInput() with { Amount = 1_000_000.01m };

        ComplianceEvaluationResult result = await sut.EvaluateAsync(input, CancellationToken.None);

        result.IsCompliant.Should().BeFalse("because the workflow caps a single transaction at 1,000,000");
        result.FailureReason.Should().Contain("maximum permitted single-transaction threshold");
    }

    [Fact]
    public async Task EvaluateAsync_AtExactMaxThreshold_ShouldRemainCompliant()
    {
        ComplianceRulesEvaluator sut = CreateEvaluator();
        ComplianceRuleInput input = CreateBaselineCompliantInput() with { Amount = 1_000_000.00m };

        ComplianceEvaluationResult result = await sut.EvaluateAsync(input, CancellationToken.None);

        result.IsCompliant.Should().BeTrue("because the rule is an inclusive <= boundary, not a strict less-than");
    }

    [Fact]
    public async Task EvaluateAsync_WithIdenticalSourceAndTargetAccounts_ShouldFailDistinctCounterpartiesRule()
    {
        ComplianceRulesEvaluator sut = CreateEvaluator();
        Guid sameAccountId = Guid.NewGuid();
        ComplianceRuleInput input = CreateBaselineCompliantInput() with
        {
            SourceAccountId = sameAccountId,
            TargetAccountId = sameAccountId
        };

        ComplianceEvaluationResult result = await sut.EvaluateAsync(input, CancellationToken.None);

        result.IsCompliant.Should().BeFalse("because a self-transfer must be rejected by the DistinctCounterparties rule");
        result.FailureReason.Should().Contain("must not be identical");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(999)]
    public async Task EvaluateAsync_WithUnsupportedCurrencyCode_ShouldFailSupportedCurrencyRule(int currency)
    {
        ComplianceRulesEvaluator sut = CreateEvaluator();
        ComplianceRuleInput input = CreateBaselineCompliantInput() with { Currency = currency };

        ComplianceEvaluationResult result = await sut.EvaluateAsync(input, CancellationToken.None);

        result.IsCompliant.Should().BeFalse("because currency codes outside the supported enum range must not silently pass");
        result.FailureReason.Should().Contain("not a currently supported system asset");
    }

    [Fact]
    public async Task EvaluateAsync_WithMultipleViolations_ShouldReportOnlyTheFirstViolatedRule()
    {
        ComplianceRulesEvaluator sut = CreateEvaluator();
        Guid sameAccountId = Guid.NewGuid();
        ComplianceRuleInput input = new()
        {
            TransactionId = Guid.NewGuid(),
            SourceAccountId = sameAccountId,
            TargetAccountId = sameAccountId,
            Amount = -10m,
            Currency = 1
        };

        ComplianceEvaluationResult result = await sut.EvaluateAsync(input, CancellationToken.None);

        result.IsCompliant.Should().BeFalse("because at least one of the two violated rules must fail the transaction");
        result.FailureReason.Should().NotBeNullOrWhiteSpace("because a rejected transaction must always carry an auditable reason");
    }
}
