using FluentAssertions;
using IntegrationBus.CoreLedger.Contracts.Messages.Events;
using IntegrationBus.CoreLedger.Service.Consumers;
using MassTransit;
using MassTransit.Courier.Contracts;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;

namespace IntegrationBus.CoreLedger.Service.Tests.Consumers;

/// <summary>
/// Exercises <see cref="LedgerRoutingSlipEventConsumer"/>, the central interceptor translating the in-memory Courier
/// routing slip's terminal events (Completed / Faulted) back into the Kafka-facing WriteLedgerRecordPassed /
/// WriteLedgerRecordFailed contract events, including the "TransactionId variable missing" defensive branch.
/// </summary>
public sealed class LedgerRoutingSlipEventConsumerTests
{
    private static LedgerRoutingSlipEventConsumer CreateConsumer(
        out ITopicProducer<WriteLedgerRecordPassed> passedProducer,
        out ITopicProducer<WriteLedgerRecordFailed> failedProducer)
    {
        passedProducer = Substitute.For<ITopicProducer<WriteLedgerRecordPassed>>();
        failedProducer = Substitute.For<ITopicProducer<WriteLedgerRecordFailed>>();

        return new LedgerRoutingSlipEventConsumer(
            NullLogger<LedgerRoutingSlipEventConsumer>.Instance,
            passedProducer,
            failedProducer);
    }

    private static ConsumeContext<T> BuildContext<T>(T message) where T : class
    {
        ConsumeContext<T> context = Substitute.For<ConsumeContext<T>>();
        context.Message.Returns(message);
        context.CancellationToken.Returns(CancellationToken.None);
        return context;
    }

    [Fact]
    public async Task Consume_RoutingSlipCompleted_WithTransactionIdVariable_ShouldPublishWriteLedgerRecordPassed()
    {
        Guid transactionId = Guid.NewGuid();

        RoutingSlipCompleted message = Substitute.For<RoutingSlipCompleted>();
        message.Variables.Returns(new Dictionary<string, object> { ["TransactionId"] = transactionId });

        LedgerRoutingSlipEventConsumer consumer = CreateConsumer(out ITopicProducer<WriteLedgerRecordPassed> passedProducer, out ITopicProducer<WriteLedgerRecordFailed> failedProducer);

        await consumer.Consume(BuildContext(message));

        await passedProducer.Received(1).Produce(
            Arg.Is<WriteLedgerRecordPassed>(e => e.TransactionId == transactionId && e.EntryId > 0),
            Arg.Any<CancellationToken>());
        await failedProducer.DidNotReceive().Produce(Arg.Any<WriteLedgerRecordFailed>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Consume_RoutingSlipCompleted_WithoutTransactionIdVariable_ShouldNotPublishAnything()
    {
        RoutingSlipCompleted message = Substitute.For<RoutingSlipCompleted>();
        message.Variables.Returns(new Dictionary<string, object>());

        LedgerRoutingSlipEventConsumer consumer = CreateConsumer(out ITopicProducer<WriteLedgerRecordPassed> passedProducer, out _);

        await consumer.Consume(BuildContext(message));

        await passedProducer.DidNotReceive().Produce(Arg.Any<WriteLedgerRecordPassed>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Consume_RoutingSlipFaulted_WithTransactionIdVariable_ShouldPublishWriteLedgerRecordFailed()
    {
        Guid transactionId = Guid.NewGuid();

        RoutingSlipFaulted message = Substitute.For<RoutingSlipFaulted>();
        message.Variables.Returns(new Dictionary<string, object> { ["TransactionId"] = transactionId });

        LedgerRoutingSlipEventConsumer consumer = CreateConsumer(out ITopicProducer<WriteLedgerRecordPassed> passedProducer, out ITopicProducer<WriteLedgerRecordFailed> failedProducer);

        await consumer.Consume(BuildContext(message));

        await failedProducer.Received(1).Produce(
            Arg.Is<WriteLedgerRecordFailed>(e => e.TransactionId == transactionId),
            Arg.Any<CancellationToken>());
        await passedProducer.DidNotReceive().Produce(Arg.Any<WriteLedgerRecordPassed>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Consume_RoutingSlipFaulted_WithoutTransactionIdVariable_ShouldNotPublishAnything()
    {
        RoutingSlipFaulted message = Substitute.For<RoutingSlipFaulted>();
        message.Variables.Returns(new Dictionary<string, object>());

        LedgerRoutingSlipEventConsumer consumer = CreateConsumer(out _, out ITopicProducer<WriteLedgerRecordFailed> failedProducer);

        await consumer.Consume(BuildContext(message));

        await failedProducer.DidNotReceive().Produce(Arg.Any<WriteLedgerRecordFailed>(), Arg.Any<CancellationToken>());
    }
}
