using FluentAssertions;
using IntegrationBus.Contracts.Security;
using IntegrationBus.Shared.Security;
using MassTransit;
using NSubstitute;

namespace IntegrationBus.Shared.Tests.Security;

/// <summary>
/// Exercises <see cref="SensitiveDataMaskingFilter{T}"/>: for a message carrying <see cref="SensitiveDataAttribute"/>
/// properties, it must mirror a masked shadow copy to the security topic BEFORE handing the untouched original
/// message to the real consumer pipe; for a message with no tagged properties, it must skip the mirror entirely.
/// </summary>
public sealed class SensitiveDataMaskingFilterTests
{
    public sealed record SamplePayload
    {
        [SensitiveData]
        public Guid AccountId { get; init; }

        [SensitiveData]
        public decimal Amount { get; init; }

        public string Currency { get; init; } = string.Empty;
    }

    public sealed record NonSensitivePayload
    {
        public string Currency { get; init; } = string.Empty;
    }

    private static ConsumeContext<T> BuildContext<T>(T message, Guid? messageId = null) where T : class
    {
        ConsumeContext<T> context = Substitute.For<ConsumeContext<T>>();
        context.Message.Returns(message);
        context.MessageId.Returns(messageId);
        context.CancellationToken.Returns(CancellationToken.None);
        return context;
    }

    [Fact]
    public async Task Send_WithSensitiveDataAttributedProperties_ShouldPublishMaskedValuesBeforeForwardingToNext()
    {
        Guid accountId = Guid.NewGuid();
        Guid messageId = Guid.NewGuid();
        SamplePayload message = new() { AccountId = accountId, Amount = 42.50m, Currency = "USD" };

        IHmacMaskingService maskingService = Substitute.For<IHmacMaskingService>();
        maskingService.MaskValue(accountId).Returns("masked-account-id");
        maskingService.MaskValue(42.50m).Returns("masked-amount");

        ISecurityTopicPublisher securityPublisher = Substitute.For<ISecurityTopicPublisher>();

        SensitiveDataMaskingFilter<SamplePayload> filter = new("balance-topic", maskingService, securityPublisher);

        ConsumeContext<SamplePayload> context = BuildContext(message, messageId);
        IPipe<ConsumeContext<SamplePayload>> next = Substitute.For<IPipe<ConsumeContext<SamplePayload>>>();

        List<string> callOrder = [];
        securityPublisher
            .PublishMaskedAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<IReadOnlyDictionary<string, string>>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask)
            .AndDoes(_ => callOrder.Add("publish"));
        next.Send(Arg.Any<ConsumeContext<SamplePayload>>())
            .Returns(Task.CompletedTask)
            .AndDoes(_ => callOrder.Add("next"));

        await filter.Send(context, next);

        await securityPublisher.Received(1).PublishMaskedAsync(
            "balance-topic",
            messageId.ToString(),
            Arg.Is<IReadOnlyDictionary<string, string>>(fields =>
                fields.Count == 2 &&
                fields["AccountId"] == "masked-account-id" &&
                fields["Amount"] == "masked-amount"),
            Arg.Any<CancellationToken>());

        await next.Received(1).Send(context);

        callOrder.Should().Equal(["publish", "next"],
            "because the masked shadow copy must be mirrored to the security topic before the original, unmasked message reaches the real consumer");
    }

    [Fact]
    public async Task Send_WithNoMessageId_ShouldFallBackToARandomPartitionKey()
    {
        SamplePayload message = new() { AccountId = Guid.NewGuid(), Amount = 1m, Currency = "USD" };

        IHmacMaskingService maskingService = Substitute.For<IHmacMaskingService>();
        maskingService.MaskValue(Arg.Any<object?>()).Returns("masked");

        ISecurityTopicPublisher securityPublisher = Substitute.For<ISecurityTopicPublisher>();
        securityPublisher
            .PublishMaskedAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<IReadOnlyDictionary<string, string>>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        SensitiveDataMaskingFilter<SamplePayload> filter = new("balance-topic", maskingService, securityPublisher);

        ConsumeContext<SamplePayload> context = BuildContext(message, messageId: null);
        IPipe<ConsumeContext<SamplePayload>> next = Substitute.For<IPipe<ConsumeContext<SamplePayload>>>();
        next.Send(Arg.Any<ConsumeContext<SamplePayload>>()).Returns(Task.CompletedTask);

        await filter.Send(context, next);

        await securityPublisher.Received(1).PublishMaskedAsync(
            "balance-topic",
            Arg.Is<string>(key => IsNonEmptyGuid(key)),
            Arg.Any<IReadOnlyDictionary<string, string>>(),
            Arg.Any<CancellationToken>());
    }

    private static bool IsNonEmptyGuid(string key)
    {
        return Guid.TryParse(key, out Guid parsed) && parsed != Guid.Empty;
    }

    [Fact]
    public async Task Send_WithNoSensitiveDataAttributedProperties_ShouldSkipTheSecurityPublishEntirely()
    {
        NonSensitivePayload message = new() { Currency = "USD" };

        IHmacMaskingService maskingService = Substitute.For<IHmacMaskingService>();
        ISecurityTopicPublisher securityPublisher = Substitute.For<ISecurityTopicPublisher>();

        SensitiveDataMaskingFilter<NonSensitivePayload> filter = new("fx-topic", maskingService, securityPublisher);

        ConsumeContext<NonSensitivePayload> context = BuildContext(message);
        IPipe<ConsumeContext<NonSensitivePayload>> next = Substitute.For<IPipe<ConsumeContext<NonSensitivePayload>>>();
        next.Send(Arg.Any<ConsumeContext<NonSensitivePayload>>()).Returns(Task.CompletedTask);

        await filter.Send(context, next);

        await securityPublisher.DidNotReceive().PublishMaskedAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<IReadOnlyDictionary<string, string>>(), Arg.Any<CancellationToken>());
        await next.Received(1).Send(context);
    }
}
