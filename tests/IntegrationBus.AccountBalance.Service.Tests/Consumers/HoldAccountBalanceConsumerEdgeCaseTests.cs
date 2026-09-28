using Dapper;
using FluentAssertions;
using IntegrationBus.AccountBalance.Contracts.Messages.Commands;
using IntegrationBus.AccountBalance.Contracts.Messages.Events;
using IntegrationBus.AccountBalance.Service.Consumers;
using IntegrationBus.AccountBalance.Service.DbContexts;
using IntegrationBus.AccountBalance.Service.Entities;
using IntegrationBus.AccountBalance.Service.Enums;
using IntegrationBus.AccountBalance.Service.Providers;
using IntegrationBus.AccountBalance.Service.Tests.Fixtures;
using IntegrationBus.Contracts.Enums;
using MassTransit;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NSubstitute;
using RedLockNet;
using RedLockNet.SERedis;
using RedLockNet.SERedis.Configuration;
using StackExchange.Redis;
using Testcontainers.Redis;

namespace IntegrationBus.AccountBalance.Service.Tests.Consumers;

/// <summary>
/// Covers the two concurrency-related edge cases called out explicitly for this stream: a redelivered (duplicate)
/// Kafka message must not double-debit an account, and two replicas racing to hold funds against the same account
/// must be serialized by the Redis distributed lock rather than both succeeding past the balance check.
/// Uses a real Postgres (Testcontainers) and a real Redis (Testcontainers) so the actual RedLock.net + Npgsql
/// code paths run exactly as in production -- only the Kafka <see cref="ConsumeContext{T}"/> and the outbound
/// <see cref="ITopicProducer{T}"/> event producers are substituted, since no Kafka broker is involved here.
/// </summary>
public sealed class HoldAccountBalanceConsumerEdgeCaseTests : IClassFixture<DatabaseFixture>, IAsyncLifetime, IDisposable
{
    private readonly DatabaseFixture _postgresFixture;
    private readonly RedisContainer _redis = new RedisBuilder("redis:7-alpine").Build();
    private IDistributedLockFactory _lockFactory = null!;
    private BalanceDbContext _dbContext = null!;

    public HoldAccountBalanceConsumerEdgeCaseTests(DatabaseFixture postgresFixture)
    {
        _postgresFixture = postgresFixture;
    }

    public async Task InitializeAsync()
    {
        await _redis.StartAsync();

        ConnectionMultiplexer multiplexer = await ConnectionMultiplexer.ConnectAsync(_redis.GetConnectionString());
        _lockFactory = RedLockFactory.Create([new RedLockMultiplexer(multiplexer)]);

        DbContextOptions<BalanceDbContext> options = new DbContextOptionsBuilder<BalanceDbContext>()
            .UseNpgsql(_postgresFixture.Container.GetConnectionString())
            .Options;
        _dbContext = new BalanceDbContext(options);

        await using NpgsqlConnection connection = new(_postgresFixture.Container.GetConnectionString());
        await connection.OpenAsync();

        await connection.ExecuteAsync($"""
            CREATE TABLE IF NOT EXISTS "{nameof(BalanceDbContext.Accounts)}" (
                "{nameof(AccountEntity.Id)}" UUID PRIMARY KEY,
                "{nameof(AccountEntity.Currency)}" INT NOT NULL,
                "{nameof(AccountEntity.CreatedAt)}" TIMESTAMP WITH TIME ZONE NOT NULL
            );
            """);

        await connection.ExecuteAsync($"""
            CREATE TABLE IF NOT EXISTS "{nameof(BalanceDbContext.JournalEntries)}" (
                "{nameof(AccountJournalEntryEntity.SourceAccountId)}" UUID NOT NULL,
                "{nameof(AccountJournalEntryEntity.TargetAccountId)}" UUID,
                "{nameof(AccountJournalEntryEntity.SequenceNumber)}" BIGINT NOT NULL,
                "{nameof(AccountJournalEntryEntity.AmountDelta)}" NUMERIC(18,2) NOT NULL,
                "{nameof(AccountJournalEntryEntity.EntryType)}" INT NOT NULL,
                "{nameof(AccountJournalEntryEntity.TransactionId)}" UUID NOT NULL,
                "{nameof(AccountJournalEntryEntity.TimestampUtc)}" TIMESTAMP WITH TIME ZONE NOT NULL,
                PRIMARY KEY ("{nameof(AccountJournalEntryEntity.SourceAccountId)}", "{nameof(AccountJournalEntryEntity.SequenceNumber)}")
            );
            """);

        await connection.ExecuteAsync($"""
            CREATE TABLE IF NOT EXISTS "{nameof(BalanceDbContext.Snapshots)}" (
                "{nameof(AccountSnapshotEntity.AccountId)}" UUID NOT NULL,
                "{nameof(AccountSnapshotEntity.SequenceNumber)}" BIGINT NOT NULL,
                "{nameof(AccountSnapshotEntity.SnapshotBalance)}" NUMERIC(18,2) NOT NULL,
                "{nameof(AccountSnapshotEntity.CapturedAtUtc)}" TIMESTAMP WITH TIME ZONE NOT NULL,
                PRIMARY KEY ("{nameof(AccountSnapshotEntity.AccountId)}", "{nameof(AccountSnapshotEntity.SequenceNumber)}")
            );
            """);
    }

    public async Task DisposeAsync()
    {
        await _dbContext.DisposeAsync();
        await _redis.DisposeAsync();
    }

    public void Dispose()
    {
        _dbContext?.Dispose();
    }

    private HoldAccountBalanceConsumer CreateConsumer(
        out ITopicProducer<HoldAccountBalancePassed> passedProducer,
        out ITopicProducer<HoldAccountBalanceFailed> failedProducer)
    {
        passedProducer = Substitute.For<ITopicProducer<HoldAccountBalancePassed>>();
        failedProducer = Substitute.For<ITopicProducer<HoldAccountBalanceFailed>>();

        return new HoldAccountBalanceConsumer(
            NullLogger<HoldAccountBalanceConsumer>.Instance,
            _dbContext,
            new AccountStateReconstructor(),
            _lockFactory,
            passedProducer,
            failedProducer);
    }

    private static ConsumeContext<HoldAccountBalance> BuildContext(HoldAccountBalance message)
    {
        ConsumeContext<HoldAccountBalance> context = Substitute.For<ConsumeContext<HoldAccountBalance>>();
        context.Message.Returns(message);
        context.CancellationToken.Returns(CancellationToken.None);
        return context;
    }

    private async Task SeedAccountAsync(Guid accountId, Currency currency)
    {
        await using NpgsqlConnection connection = new(_postgresFixture.Container.GetConnectionString());
        await connection.OpenAsync();
        await connection.ExecuteAsync($"""INSERT INTO "{nameof(BalanceDbContext.Accounts)}" ("{nameof(AccountEntity.Id)}", "{nameof(AccountEntity.Currency)}", "{nameof(AccountEntity.CreatedAt)}") VALUES (@Id, @Currency, @CreatedAt)""",
            new { Id = accountId, Currency = (int)currency, CreatedAt = DateTime.UtcNow });
    }

    private async Task SeedDirectDepositAsync(Guid accountId, decimal amount)
    {
        await using NpgsqlConnection connection = new(_postgresFixture.Container.GetConnectionString());
        await connection.OpenAsync();
        await connection.ExecuteAsync($"""
            INSERT INTO "{nameof(BalanceDbContext.JournalEntries)}"
            ("{nameof(AccountJournalEntryEntity.SourceAccountId)}", "{nameof(AccountJournalEntryEntity.SequenceNumber)}", "{nameof(AccountJournalEntryEntity.AmountDelta)}", "{nameof(AccountJournalEntryEntity.EntryType)}", "{nameof(AccountJournalEntryEntity.TransactionId)}", "{nameof(AccountJournalEntryEntity.TimestampUtc)}")
            VALUES (@SourceAccountId, 1, @Amount, @EntryType, @TransactionId, @TimestampUtc)
            """,
            new { SourceAccountId = accountId, Amount = amount, EntryType = (int)JournalEntryType.DirectDeposit, TransactionId = Guid.NewGuid(), TimestampUtc = DateTime.UtcNow });
    }

    private async Task<long> CountHoldEntriesAsync(Guid transactionId)
    {
        await using NpgsqlConnection connection = new(_postgresFixture.Container.GetConnectionString());
        await connection.OpenAsync();
        return await connection.ExecuteScalarAsync<long>(
            $"""SELECT count(*) FROM "{nameof(BalanceDbContext.JournalEntries)}" WHERE "{nameof(AccountJournalEntryEntity.TransactionId)}" = @TransactionId AND "{nameof(AccountJournalEntryEntity.EntryType)}" = @EntryType""",
            new { TransactionId = transactionId, EntryType = (int)JournalEntryType.Hold });
    }

    [Fact]
    public async Task Consume_WithRedeliveredDuplicateMessage_ShouldAppendTheHoldExactlyOnce()
    {
        Guid sourceAccountId = Guid.NewGuid();
        Guid targetAccountId = Guid.NewGuid();
        Guid transactionId = Guid.NewGuid();

        await SeedAccountAsync(sourceAccountId, Currency.USD);
        await SeedAccountAsync(targetAccountId, Currency.USD);
        await SeedDirectDepositAsync(sourceAccountId, 500m);

        HoldAccountBalance message = new()
        {
            TransactionId = transactionId,
            AccountFromId = sourceAccountId,
            AccountToId = targetAccountId,
            Amount = 100m,
            Currency = Currency.USD
        };

        HoldAccountBalanceConsumer consumer = CreateConsumer(out ITopicProducer<HoldAccountBalancePassed> passedProducer, out _);

        await consumer.Consume(BuildContext(message));
        await consumer.Consume(BuildContext(message));

        long holdCount = await CountHoldEntriesAsync(transactionId);

        holdCount.Should().Be(1,
            "because a Kafka at-least-once redelivery of the exact same command must be replayed idempotently, not debit the account twice");

        await passedProducer.Received(2).Produce(
            Arg.Is<HoldAccountBalancePassed>(e => e.TransactionId == transactionId),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Consume_WithTwoConcurrentHoldsOnTheSameAccount_ShouldSerializeViaDistributedLockAndPreventOverdraft()
    {
        Guid sourceAccountId = Guid.NewGuid();
        Guid targetAccountId = Guid.NewGuid();

        await SeedAccountAsync(sourceAccountId, Currency.USD);
        await SeedAccountAsync(targetAccountId, Currency.USD);

        // Seed available capacity of exactly 100 via a direct deposit-style journal entry
        await using (NpgsqlConnection seedConnection = new(_postgresFixture.Container.GetConnectionString()))
        {
            await seedConnection.OpenAsync();
            await seedConnection.ExecuteAsync($"""
                INSERT INTO "{nameof(BalanceDbContext.JournalEntries)}"
                ("{nameof(AccountJournalEntryEntity.SourceAccountId)}", "{nameof(AccountJournalEntryEntity.SequenceNumber)}", "{nameof(AccountJournalEntryEntity.AmountDelta)}", "{nameof(AccountJournalEntryEntity.EntryType)}", "{nameof(AccountJournalEntryEntity.TransactionId)}", "{nameof(AccountJournalEntryEntity.TimestampUtc)}")
                VALUES (@SourceAccountId, 1, 100.00, @EntryType, @TransactionId, @TimestampUtc)
                """,
                new { SourceAccountId = sourceAccountId, EntryType = (int)JournalEntryType.DirectDeposit, TransactionId = Guid.NewGuid(), TimestampUtc = DateTime.UtcNow });
        }

        // Two replicas race to hold 80 each against an account that only has 100 available -- at most one may succeed
        HoldAccountBalance firstMessage = new() { TransactionId = Guid.NewGuid(), AccountFromId = sourceAccountId, AccountToId = targetAccountId, Amount = 80m, Currency = Currency.USD };
        HoldAccountBalance secondMessage = new() { TransactionId = Guid.NewGuid(), AccountFromId = sourceAccountId, AccountToId = targetAccountId, Amount = 80m, Currency = Currency.USD };

        HoldAccountBalanceConsumer firstConsumer = CreateConsumer(out ITopicProducer<HoldAccountBalancePassed> firstPassed, out ITopicProducer<HoldAccountBalanceFailed> firstFailed);
        HoldAccountBalanceConsumer secondConsumer = CreateConsumer(out ITopicProducer<HoldAccountBalancePassed> secondPassed, out ITopicProducer<HoldAccountBalanceFailed> secondFailed);

        await Task.WhenAll(
            firstConsumer.Consume(BuildContext(firstMessage)),
            secondConsumer.Consume(BuildContext(secondMessage)));

        int passedCount = firstPassed.ReceivedCalls().Count() + secondPassed.ReceivedCalls().Count();
        int failedCount = firstFailed.ReceivedCalls().Count() + secondFailed.ReceivedCalls().Count();

        passedCount.Should().Be(1,
            "because the Redis distributed lock must serialize the two concurrent holds so only one observes sufficient available funds");
        failedCount.Should().Be(1,
            "because the second, serialized hold must now see the reduced balance and correctly reject as insufficient funds");
    }
}
