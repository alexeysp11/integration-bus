using System.Collections.Concurrent;
using MassTransit;

namespace IntegrationBus.SagaOrchestrator.Service.Tests.Fakes;

/// <summary>
/// Stands in for the real Kafka Rider <see cref="ITopicProducer{TValue}"/> so saga state machine activities can be
/// exercised through a pure in-memory <c>MassTransit.Testing</c> harness, without a Kafka broker. Every "produced"
/// command is simply recorded for assertions.
/// </summary>
public sealed class RecordingTopicProducer<T> : ITopicProducer<T> where T : class
{
    public ConcurrentQueue<T> Produced { get; } = new();

    public Task Produce(T message, CancellationToken cancellationToken = default) =>
        Produce(message, Pipe.Empty<KafkaSendContext<T>>(), cancellationToken);

    public Task Produce(object values, CancellationToken cancellationToken = default) =>
        Produce((T)values, cancellationToken);

    public Task Produce(T message, IPipe<KafkaSendContext<T>> pipe, CancellationToken cancellationToken = default)
    {
        Produced.Enqueue(message);
        return Task.CompletedTask;
    }

    public Task Produce(object values, IPipe<KafkaSendContext<T>> pipe, CancellationToken cancellationToken = default)
    {
        Produced.Enqueue((T)values);
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
