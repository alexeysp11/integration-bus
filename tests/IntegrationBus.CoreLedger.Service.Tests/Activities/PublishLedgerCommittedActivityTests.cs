using FluentAssertions;
using IntegrationBus.CoreLedger.Service.Activities;
using IntegrationBus.CoreLedger.Service.Models;
using MassTransit;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;

namespace IntegrationBus.CoreLedger.Service.Tests.Activities;

/// <summary>
/// Exercises <see cref="PublishLedgerCommittedActivity"/> -- the terminal, execute-only stage of the Core Ledger
/// Courier routing slip. It has no persistent side effects of its own (the actual Kafka event is dispatched one
/// layer up by <c>LedgerRoutingSlipEventConsumer</c> once the whole routing slip completes), so this test verifies
/// it always reports completion and never faults the routing slip.
/// </summary>
public sealed class PublishLedgerCommittedActivityTests
{
    [Fact]
    public async Task Execute_ShouldAlwaysReportCompletion()
    {
        PublishLedgerCommittedArguments arguments = new()
        {
            TransactionId = Guid.NewGuid(),
            Amount = 75m,
            Currency = (int)IntegrationBus.Contracts.Enums.Currency.USD
        };

        ExecuteContext<PublishLedgerCommittedArguments> context = Substitute.For<ExecuteContext<PublishLedgerCommittedArguments>>();
        context.Arguments.Returns(arguments);

        ExecutionResult expectedResult = Substitute.For<ExecutionResult>();
        context.Completed().Returns(expectedResult);

        PublishLedgerCommittedActivity activity = new(NullLogger<PublishLedgerCommittedActivity>.Instance);

        ExecutionResult actualResult = await activity.Execute(context);

        actualResult.Should().BeSameAs(expectedResult);
        context.Received(1).Completed();
    }
}
