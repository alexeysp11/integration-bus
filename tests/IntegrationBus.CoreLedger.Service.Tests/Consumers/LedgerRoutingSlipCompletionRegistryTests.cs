using FluentAssertions;
using IntegrationBus.CoreLedger.Service.Consumers;

namespace IntegrationBus.CoreLedger.Service.Tests.Consumers;

/// <summary>
/// Exercises <see cref="LedgerRoutingSlipCompletionRegistry"/>, which keeps the Kafka offset of a ledger command
/// uncommitted until its local routing slip reached a terminal state.
/// </summary>
public sealed class LedgerRoutingSlipCompletionRegistryTests
{
    [Fact]
    public async Task Complete_ForARegisteredSlip_ShouldReleaseTheWaitingTask()
    {
        LedgerRoutingSlipCompletionRegistry registry = new();
        Guid trackingNumber = Guid.NewGuid();

        Task waiting = registry.Register(trackingNumber);
        waiting.IsCompleted.Should().BeFalse("because the slip has not reached a terminal state yet");

        registry.Complete(trackingNumber);

        await waiting.WaitAsync(TimeSpan.FromSeconds(5));
        waiting.IsCompletedSuccessfully.Should().BeTrue("because Complete must release the consumer holding the Kafka offset");
    }

    [Fact]
    public void Complete_ForAnUnknownSlip_ShouldBeIgnored()
    {
        LedgerRoutingSlipCompletionRegistry registry = new();

        Action act = () => registry.Complete(Guid.NewGuid());

        act.Should().NotThrow("because terminal events of slips started by another replica must be ignored");
    }

    [Fact]
    public async Task Forget_ShouldPreventLaterCompletionFromReleasingTheTask()
    {
        LedgerRoutingSlipCompletionRegistry registry = new();
        Guid trackingNumber = Guid.NewGuid();
        Task waiting = registry.Register(trackingNumber);

        registry.Forget(trackingNumber);
        registry.Complete(trackingNumber);

        Func<Task> act = () => waiting.WaitAsync(TimeSpan.FromMilliseconds(200));
        await act.Should().ThrowAsync<TimeoutException>("because a forgotten registration is no longer completed");
    }
}
