using Dapper;
using FluentAssertions;
using IntegrationBus.AccountBalance.Contracts.Messages.Commands;
using IntegrationBus.AccountBalance.Contracts.Messages.Events;
using IntegrationBus.AccountBalance.Service.Consumers;
using IntegrationBus.AccountBalance.Service.DbContexts;
using IntegrationBus.AccountBalance.Service.Entities;
using IntegrationBus.AccountBalance.Service.Enums;
using IntegrationBus.AccountBalance.Service.Tests.Fixtures;
using IntegrationBus.Contracts.Enums;
using MassTransit;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NSubstitute;

namespace IntegrationBus.AccountBalance.Service.Tests.Consumers;

/// <summary>
/// Covers the Kafka at-least-once redelivery edge case for <see cref="TopUpAccountBalanceConsumer"/>: a duplicate
/// replenishment command for a transaction that already posted its direct deposit entry must be replayed without
/// crediting the account a second time. Uses a real Postgres (Testcontainers) so the actual Npgsql/Dapper
/// transaction code path runs exactly as in production -- only the Kafka <see cref="ConsumeContext{T}"/> and the
/// outbound <see cref="ITopicProducer{T}"/> event producers are substituted.
/// </summary>
public sealed class TopUpAccountBalanceConsumerEdgeCaseTests : IClassFixture<DatabaseFixture>, IAsyncLifetime, IDisposable
{
    private readonly DatabaseFixture _postgresFixture;
    private BalanceDbContext _dbContext = null!;

    public TopUpAccountBalanceConsumerEdgeCaseTests(DatabaseFixture postgresFixture)
    {
        _postgresFixture = postgresFixture;
    }

    public async Task InitializeAsync()
    {
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

    public async Task DisposeAsync() => await _dbContext.DisposeAsync();

    public void Dispose() => _dbContext?.Dispose();

    private TopUpAccountBalanceConsumer CreateConsumer(
        out ITopicProducer<TopUpAccountBalancePassed> passedProducer,
        out ITopicProducer<TopUpAccountBalanceFailed> failedProducer)
    {
        passedProducer = Substitute.For<ITopicProducer<TopUpAccountBalancePassed>>();
        failedProducer = Substitute.For<ITopicProducer<TopUpAccountBalanceFailed>>();

        return new TopUpAccountBalanceConsumer(
            _dbContext,
            NullLogger<TopUpAccountBalanceConsumer>.Instance,
            passedProducer,
            failedProducer);
    }

    private static ConsumeContext<TopUpAccountBalance> BuildContext(TopUpAccountBalance message)
    {
        ConsumeContext<TopUpAccountBalance> context = Substitute.For<ConsumeContext<TopUpAccountBalance>>();
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

    private async Task<long> CountDepositEntriesAsync(Guid transactionId)
    {
        await using NpgsqlConnection connection = new(_postgresFixture.Container.GetConnectionString());
        await connection.OpenAsync();
        return await connection.ExecuteScalarAsync<long>(
            $"""SELECT count(*) FROM "{nameof(BalanceDbContext.JournalEntries)}" WHERE "{nameof(AccountJournalEntryEntity.TransactionId)}" = @TransactionId AND "{nameof(AccountJournalEntryEntity.EntryType)}" = @EntryType""",
            new { TransactionId = transactionId, EntryType = (int)JournalEntryType.DirectDeposit });
    }

    [Fact]
    public async Task Consume_WithRedeliveredDuplicateMessage_ShouldAppendTheDepositExactlyOnce()
    {
        Guid accountId = Guid.NewGuid();
        Guid transactionId = Guid.NewGuid();

        await SeedAccountAsync(accountId, Currency.USD);

        TopUpAccountBalance message = new()
        {
            TransactionId = transactionId,
            AccountId = accountId,
            Amount = 150m,
            Currency = Currency.USD
        };

        TopUpAccountBalanceConsumer consumer = CreateConsumer(out ITopicProducer<TopUpAccountBalancePassed> passedProducer, out _);

        await consumer.Consume(BuildContext(message));
        await consumer.Consume(BuildContext(message));

        long depositCount = await CountDepositEntriesAsync(transactionId);

        depositCount.Should().Be(1,
            "because a Kafka at-least-once redelivery of the exact same top-up command must be replayed idempotently, not credit the account twice");

        await passedProducer.Received(2).Produce(
            Arg.Is<TopUpAccountBalancePassed>(e => e.TransactionId == transactionId),
            Arg.Any<CancellationToken>());
    }
}
