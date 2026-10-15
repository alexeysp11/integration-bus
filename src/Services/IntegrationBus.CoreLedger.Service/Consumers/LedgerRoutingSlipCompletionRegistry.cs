using System.Collections.Concurrent;

namespace IntegrationBus.CoreLedger.Service.Consumers;

/// <summary>
/// Correlates a locally executed Courier routing slip with the Kafka consumer that started it, so the consumer can
/// hold its Kafka offset uncommitted until the slip has reached a terminal state.
/// </summary>
/// <remarks>
/// Design and failure semantics: <c>docs/chaos-engineering/README.md</c> (relative to the repository root).
/// </remarks>
public sealed class LedgerRoutingSlipCompletionRegistry
{
    private readonly ConcurrentDictionary<Guid, TaskCompletionSource> _pending = new();

    /// <summary>
    /// Registers a routing slip and returns a task that completes once <see cref="Complete"/> is called for it.
    /// </summary>
    /// <param name="trackingNumber">The routing slip tracking number.</param>
    /// <returns>A task that completes when the slip reached a terminal state and its outcome event was published.</returns>
    public Task Register(Guid trackingNumber)
    {
        TaskCompletionSource completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        _pending[trackingNumber] = completion;
        return completion.Task;
    }

    /// <summary>
    /// Marks a routing slip as finished; unknown tracking numbers are ignored.
    /// </summary>
    /// <param name="trackingNumber">The routing slip tracking number.</param>
    public void Complete(Guid trackingNumber)
    {
        if (_pending.TryRemove(trackingNumber, out TaskCompletionSource? completion))
        {
            completion.TrySetResult();
        }
    }

    /// <summary>
    /// Drops the registration of a routing slip that is no longer awaited.
    /// </summary>
    /// <param name="trackingNumber">The routing slip tracking number.</param>
    public void Forget(Guid trackingNumber) => _pending.TryRemove(trackingNumber, out _);
}
