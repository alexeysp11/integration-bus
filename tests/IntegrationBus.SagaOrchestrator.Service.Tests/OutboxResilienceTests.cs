using FluentAssertions;
using IntegrationBus.AccountBalance.Contracts.Messages.Commands;
using IntegrationBus.AccountBalance.Contracts.Messages.Events;
using IntegrationBus.Compliance.Contracts.Messages.Commands;
using IntegrationBus.Compliance.Contracts.Messages.Events;
using IntegrationBus.Contracts.Enums;
using IntegrationBus.CoreLedger.Contracts.Messages.Commands;
using IntegrationBus.SagaOrchestrator.Contracts.Messages.Commands;
using IntegrationBus.SagaOrchestrator.Service.DbContexts;
using IntegrationBus.SagaOrchestrator.Service.Sagas;
using IntegrationBus.SagaOrchestrator.Service.Tests.Fakes;
using MassTransit;
using MassTransit.Testing;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Testcontainers.PostgreSql;

namespace IntegrationBus.SagaOrchestrator.Service.Tests;

/// <summary>
/// Exercises the exact MassTransit Transactional Outbox / Consumer Inbox wiring from
/// <c>IntegrationBus.SagaOrchestrator.Service.Program</c> (<c>AddEntityFrameworkOutbox&lt;SagaDbContext&gt;</c> +
/// <c>EntityFrameworkRepository</c>) against a real Postgres instance, to prove the edge case explicitly required
/// for this stream: a broker outage mid-saga must not leave a partially-advanced (corrupted) saga instance, and
/// Kafka's at-least-once redelivery of the same event once the broker recovers must complete the transition
/// exactly once -- not duplicate the outbound command.
/// </summary>
public sealed class OutboxResilienceTests : IAsyncLifetime
{
    private readonly PostgreSqlBuilder _postgresBuilder = new PostgreSqlBuilder("postgres:16")
        .WithDatabase("saga_outbox_test")
        .WithUsername("postgres")
        .WithPassword("postgres");

    private PostgreSqlContainer _postgres = null!;
    private ServiceProvider _provider = null!;
    private ITestHarness _harness = null!;

    public async Task InitializeAsync()
    {
        _postgres = _postgresBuilder.Build();
        await _postgres.StartAsync();

        ServiceCollection services = new();

        services.AddDbContext<SagaDbContext>(o => o.UseNpgsql(_postgres.GetConnectionString()));

        RegisterRecordingProducer<HoldAccountBalance>(services);
        RegisterRecordingProducer<CheckComplianceLimits>(services);
        RegisterRecordingProducer<ConfirmAccountBalance>(services);
        RegisterRecordingProducer<ReleaseAccountBalance>(services);

        // The one activity this test deliberately destabilizes: WriteLedgerRecord dispatch, standing in for
        // "Kafka was unreachable when the saga tried to kick off the Core Ledger step".
        services.AddSingleton<FlakyTopicProducer<WriteLedgerRecord>>(_ => new FlakyTopicProducer<WriteLedgerRecord>(failFirstNCalls: 1));
        services.AddSingleton<ITopicProducer<WriteLedgerRecord>>(sp => sp.GetRequiredService<FlakyTopicProducer<WriteLedgerRecord>>());

        services.AddMassTransitTestHarness(x =>
        {
            x.AddEntityFrameworkOutbox<SagaDbContext>(o =>
            {
                o.UsePostgres();
                o.UseBusOutbox();
            });

            x.AddSagaStateMachine<TransactionSagaStateMachine, TransactionSagaInstance>()
                .EntityFrameworkRepository(r =>
                {
                    r.ExistingDbContext<SagaDbContext>();
                    r.UsePostgres();
                });

            x.UsingInMemory((context, cfg) =>
            {
                // Mirrors production's per-topic-endpoint wiring (UseEntityFrameworkOutbox + ConfigureSaga), just
                // collapsed onto a single in-memory receive endpoint instead of one Kafka topic endpoint per event.
                cfg.ReceiveEndpoint("saga-outbox-test", e =>
                {
                    e.UseEntityFrameworkOutbox<SagaDbContext>(context);
                    e.ConfigureSaga<TransactionSagaInstance>(context);
                });
            });
        });

        _provider = services.BuildServiceProvider(true);

        await using (AsyncServiceScope scope = _provider.CreateAsyncScope())
        {
            SagaDbContext dbContext = scope.ServiceProvider.GetRequiredService<SagaDbContext>();
            await dbContext.Database.MigrateAsync();
        }

        _harness = _provider.GetRequiredService<ITestHarness>();
        await _harness.Start();
    }

    public async Task DisposeAsync()
    {
        await _harness.Stop();
        await _provider.DisposeAsync();
        await _postgres.DisposeAsync();
    }

    private static void RegisterRecordingProducer<T>(ServiceCollection services) where T : class
    {
        services.AddSingleton<RecordingTopicProducer<T>>();
        services.AddSingleton<ITopicProducer<T>>(sp => sp.GetRequiredService<RecordingTopicProducer<T>>());
    }

    private RecordingTopicProducer<T> Producer<T>() where T : class => _provider.GetRequiredService<RecordingTopicProducer<T>>();

    private async Task<string> GetPersistedStateAsync(Guid transactionId)
    {
        await using AsyncServiceScope scope = _provider.CreateAsyncScope();
        SagaDbContext dbContext = scope.ServiceProvider.GetRequiredService<SagaDbContext>();

        TransactionSagaInstance? instance = await dbContext.Set<TransactionSagaInstance>()
            .AsNoTracking()
            .SingleOrDefaultAsync(x => x.CorrelationId == transactionId);

        return instance?.CurrentState ?? "<no persisted row>";
    }

    [Fact]
    public async Task BrokerOutageMidSaga_ShouldRollBackTheStateTransitionInsteadOfLeavingPartialState()
    {
        Guid transactionId = Guid.NewGuid();
        FlakyTopicProducer<WriteLedgerRecord> flakyProducer = _provider.GetRequiredService<FlakyTopicProducer<WriteLedgerRecord>>();

        await _harness.Bus.Publish(new StartTransactionSaga
        {
            TransactionId = transactionId,
            SourceAccountId = Guid.NewGuid(),
            TargetAccountId = Guid.NewGuid(),
            Amount = 100m,
            Currency = Currency.USD
        });

        string stateAfterStart = await WaitForPersistedStateAsync(transactionId, "AwaitingAccountBalanceHold");
        stateAfterStart.Should().Be("AwaitingAccountBalanceHold", "sanity check before asserting the failure path");

        await _harness.Bus.Publish(new HoldAccountBalancePassed { TransactionId = transactionId, HeldAt = DateTime.UtcNow });

        // Wait until the saga is actually persisted in AwaitingComplianceLimitsCheck before destabilizing the next step.
        string stateBeforeFailure = await WaitForPersistedStateAsync(transactionId, "AwaitingComplianceLimitsCheck");
        stateBeforeFailure.Should().Be("AwaitingComplianceLimitsCheck");

        // This event's processing will call ProcessLedgerWriteActivity, which dispatches via the flaky producer
        // configured to throw on its first call -- simulating the broker being down right when the saga tries to
        // kick off the Core Ledger step.
        await _harness.Bus.Publish(new CheckComplianceLimitsPassed { TransactionId = transactionId, VerifiedAt = DateTime.UtcNow });

        // Give the consumer a moment to attempt (and fail) processing.
        await Task.Delay(TimeSpan.FromSeconds(2));

        string stateAfterFailedDispatch = await GetPersistedStateAsync(transactionId);
        stateAfterFailedDispatch.Should().Be("AwaitingComplianceLimitsCheck",
            "because the EF Outbox must roll back the saga's state transition atomically with the failed dispatch -- " +
            "the instance must not be left in a half-advanced, corrupted state");
        flakyProducer.Produced.Should().BeEmpty("because the simulated broker outage must have prevented the dispatch from ever reaching the (fake) broker");

        // Kafka's at-least-once delivery means the un-acked CheckComplianceLimitsPassed event gets redelivered once
        // the consumer (and, in the real system, the broker) recovers.
        await _harness.Bus.Publish(new CheckComplianceLimitsPassed { TransactionId = transactionId, VerifiedAt = DateTime.UtcNow });

        string stateAfterRedelivery = await WaitForPersistedStateAsync(transactionId, "AwaitingLedgerCommit");
        stateAfterRedelivery.Should().Be("AwaitingLedgerCommit",
            "because redelivery after the broker recovers must let the saga complete the transition it previously rolled back");

        flakyProducer.Produced.Should().ContainSingle(m => m.TransactionId == transactionId,
            "because the saga must dispatch WriteLedgerRecord exactly once overall -- the failed first attempt never left the outbox, " +
            "so the successful redelivery is not a duplicate, it is the only dispatch that ever actually happened");
    }

    private async Task<string> WaitForPersistedStateAsync(Guid transactionId, string expectedState)
    {
        string lastSeen = "<none>";

        for (int attempt = 0; attempt < 20; attempt++)
        {
            lastSeen = await GetPersistedStateAsync(transactionId);
            if (lastSeen == expectedState)
            {
                return lastSeen;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(250));
        }

        return lastSeen;
    }
}
