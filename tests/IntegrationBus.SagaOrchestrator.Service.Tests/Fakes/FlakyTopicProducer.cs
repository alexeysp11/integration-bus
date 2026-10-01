using System.Collections.Concurrent;
using MassTransit;

namespace IntegrationBus.SagaOrchestrator.Service.Tests.Fakes;

/// <summary>
/// Stands in for the real Kafka Rider <see cref="ITopicProducer{TValue}"/> and throws on the first
/// <paramref name="failFirstNCalls"/> invocations, simulating the broker being unreachable precisely when an
/// activity tries to dispatch its outbound command. Used to prove that a dispatch failure mid-saga rolls back the
/// state transition (EF Outbox atomicity) instead of leaving a partially-advanced saga instance, and that a
/// subsequent redelivery of the same triggering event (as Kafka would do for an unacked message) completes cleanly.
/// </summary>
public sealed class FlakyTopicProducer<T> : ITopicProducer<T> where T : class
{
    private int _remainingFailures;

    public FlakyTopicProducer(int failFirstNCalls)
    {
        _remainingFailures = failFirstNCalls;
    }

    public ConcurrentQueue<T> Produced { get; } = new();

    public Task Produce(T message, CancellationToken cancellationToken = default) => ProduceInternal(message);

    public Task Produce(object values, CancellationToken cancellationToken = default) => ProduceInternal((T)values);

    public Task Produce(T message, IPipe<KafkaSendContext<T>> pipe, CancellationToken cancellationToken = default) =>
        ProduceInternal(message);

    public Task Produce(object values, IPipe<KafkaSendContext<T>> pipe, CancellationToken cancellationToken = default) =>
        ProduceInternal((T)values);

    private Task ProduceInternal(T message)
    {
        if (Interlocked.Decrement(ref _remainingFailures) >= 0)
        {
            throw new InvalidOperationException("Simulated broker unavailability: dispatch did not reach Kafka.");
        }

        Produced.Enqueue(message);
        return Task.CompletedTask;
    }

    public ConnectHandle ConnectSendObserver(ISendObserver observer) => new EmptyConnectHandle();

    private sealed class EmptyConnectHandle : ConnectHandle
    {
        public void Disconnect()
        {
        }

        public void Dispose()
        {
        }
    }
}
