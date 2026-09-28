using FluentAssertions;
using IntegrationBus.AccountBalance.Contracts.Messages.Commands;
using IntegrationBus.AccountBalance.Contracts.Messages.Events;
using IntegrationBus.Compliance.Contracts.Messages.Commands;
using IntegrationBus.Compliance.Contracts.Messages.Events;
using IntegrationBus.Contracts.Enums;
using IntegrationBus.CoreLedger.Contracts.Messages.Commands;
using IntegrationBus.CoreLedger.Contracts.Messages.Events;
using IntegrationBus.SagaOrchestrator.Contracts.Messages.Commands;
using IntegrationBus.SagaOrchestrator.Service.Sagas;
using IntegrationBus.SagaOrchestrator.Service.Tests.Fakes;
using MassTransit;
using MassTransit.Testing;
using Microsoft.Extensions.DependencyInjection;

namespace IntegrationBus.SagaOrchestrator.Service.Tests;

/// <summary>
/// Exercises <see cref="TransactionSagaStateMachine"/> end to end via MassTransit's in-memory test harness --
/// no Kafka broker, Postgres, or Docker container involved. Every activity's <c>ITopicProducer&lt;T&gt;</c>
/// (the Kafka Rider boundary) is swapped for a <see cref="RecordingTopicProducer{T}"/> so the real state machine
/// and activity logic run unmodified while outbound "Kafka" dispatches are simply captured for assertions.
/// </summary>
public sealed class TransactionSagaStateMachineTests : IAsyncLifetime
{
    private ServiceProvider _provider = null!;
    private ITestHarness _harness = null!;

    public async Task InitializeAsync()
    {
        ServiceCollection services = new();

        RegisterRecordingProducer<HoldAccountBalance>(services);
        RegisterRecordingProducer<CheckComplianceLimits>(services);
        RegisterRecordingProducer<WriteLedgerRecord>(services);
        RegisterRecordingProducer<ConfirmAccountBalance>(services);
        RegisterRecordingProducer<ReleaseAccountBalance>(services);

        services.AddMassTransitTestHarness(x =>
        {
            x.AddSagaStateMachine<TransactionSagaStateMachine, TransactionSagaInstance>()
                .InMemoryRepository();
        });

        _provider = services.BuildServiceProvider(true);
        _harness = _provider.GetRequiredService<ITestHarness>();
        await _harness.Start();
    }

    public async Task DisposeAsync()
    {
        await _harness.Stop();
        await _provider.DisposeAsync();
    }

    private static void RegisterRecordingProducer<T>(ServiceCollection services) where T : class
    {
        services.AddSingleton<RecordingTopicProducer<T>>();
        services.AddSingleton<ITopicProducer<T>>(sp => sp.GetRequiredService<RecordingTopicProducer<T>>());
    }

    private RecordingTopicProducer<T> Producer<T>() where T : class => _provider.GetRequiredService<RecordingTopicProducer<T>>();

    [Fact]
    public async Task HappyPath_ShouldProgressThroughEveryStateToCompleted()
    {
        Guid transactionId = Guid.NewGuid();

        await _harness.Bus.Publish(new StartTransactionSaga
        {
            TransactionId = transactionId,
            SourceAccountId = Guid.NewGuid(),
            TargetAccountId = Guid.NewGuid(),
            Amount = 100m,
            Currency = Currency.USD
        });

        ISagaStateMachineTestHarness<TransactionSagaStateMachine, TransactionSagaInstance> sagaHarness =
            _harness.GetSagaStateMachineHarness<TransactionSagaStateMachine, TransactionSagaInstance>();

        (await sagaHarness.Exists(transactionId, x => x.AwaitingAccountBalanceHold)).Should().NotBeNull(
            "because publishing StartTransactionSaga must create the saga and immediately dispatch the Hold command");
        Producer<HoldAccountBalance>().Produced.Should().ContainSingle(m => m.TransactionId == transactionId);

        await _harness.Bus.Publish(new HoldAccountBalancePassed { TransactionId = transactionId, HeldAt = DateTime.UtcNow });

        (await sagaHarness.Exists(transactionId, x => x.AwaitingComplianceLimitsCheck)).Should().NotBeNull();
        Producer<CheckComplianceLimits>().Produced.Should().ContainSingle(m => m.TransactionId == transactionId);

        await _harness.Bus.Publish(new CheckComplianceLimitsPassed { TransactionId = transactionId, VerifiedAt = DateTime.UtcNow });

        (await sagaHarness.Exists(transactionId, x => x.AwaitingLedgerCommit)).Should().NotBeNull();
        Producer<WriteLedgerRecord>().Produced.Should().ContainSingle(m => m.TransactionId == transactionId);

        await _harness.Bus.Publish(new WriteLedgerRecordPassed { TransactionId = transactionId, EntryId = 1, CreatedAt = DateTime.UtcNow });

        (await sagaHarness.Exists(transactionId, x => x.AwaitingAccountingCommit)).Should().NotBeNull();
        Producer<ConfirmAccountBalance>().Produced.Should().ContainSingle(m => m.TransactionId == transactionId);

        await _harness.Bus.Publish(new ConfirmAccountBalancePassed { TransactionId = transactionId, ConfirmedAtUtc = DateTime.UtcNow });

        (await sagaHarness.Exists(transactionId, x => x.Completed)).Should().NotBeNull(
            "because a fully successful saga must reach the terminal Completed state");
        Producer<ReleaseAccountBalance>().Produced.Should().BeEmpty(
            "because a fully successful transaction must never trigger a compensating release");
    }

    [Fact]
    public async Task ComplianceFailure_ShouldCompensateTheAccountBalanceHoldAndTransitionToFailed()
    {
        Guid transactionId = Guid.NewGuid();

        await _harness.Bus.Publish(new StartTransactionSaga
        {
            TransactionId = transactionId,
            SourceAccountId = Guid.NewGuid(),
            TargetAccountId = Guid.NewGuid(),
            Amount = 50m,
            Currency = Currency.USD
        });

        ISagaStateMachineTestHarness<TransactionSagaStateMachine, TransactionSagaInstance> sagaHarness =
            _harness.GetSagaStateMachineHarness<TransactionSagaStateMachine, TransactionSagaInstance>();

        (await sagaHarness.Exists(transactionId, x => x.AwaitingAccountBalanceHold)).Should().NotBeNull();

        await _harness.Bus.Publish(new HoldAccountBalancePassed { TransactionId = transactionId, HeldAt = DateTime.UtcNow });
        (await sagaHarness.Exists(transactionId, x => x.AwaitingComplianceLimitsCheck)).Should().NotBeNull();

        await _harness.Bus.Publish(new CheckComplianceLimitsFailed { TransactionId = transactionId, Reason = "Amount exceeds threshold" });

        (await sagaHarness.Exists(transactionId, x => x.Failed)).Should().NotBeNull(
            "because a compliance rejection must move the saga to its terminal Failed state, not leave it hanging mid-flight");
        Producer<ReleaseAccountBalance>().Produced.Should().ContainSingle(m => m.TransactionId == transactionId,
            "because the previously held funds must be compensated exactly once when compliance rejects the transaction");
        Producer<WriteLedgerRecord>().Produced.Should().BeEmpty(
            "because a compliance failure must short-circuit the saga before it ever reaches the ledger step");
    }
}
