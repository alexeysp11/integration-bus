using Dapper;
using FluentAssertions;
using IntegrationBus.AccountBalance.Contracts.Enums;
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
/// Covers the Kafka at-least-once redelivery edge case for <see cref="ReleaseAccountBalanceConsumer"/>: a duplicate
/// compensation for a transaction whose hold was already cancelled must be replayed without crediting the source
/// account's available capacity a second time. Also covers the companion idempotency branch where no hold exists
/// at all (e.g. the saga never actually reached the hold step), which must skip cleanly instead of faulting.
/// Uses a real Postgres (Testcontainers) so the actual Npgsql/Dapper transaction code path runs exactly as in
/// production -- only the Kafka <see cref="ConsumeContext{T}"/> and the outbound <see cref="ITopicProducer{T}"/>
/// event producers are substituted.
/// </summary>
public sealed class ReleaseAccountBalanceConsumerEdgeCaseTests : IClassFixture<DatabaseFixture>, IAsyncLifetime, IDisposable
{
    private readonly DatabaseFixture _postgresFixture;
    private BalanceDbContext _dbContext = null!;

    public ReleaseAccountBalanceConsumerEdgeCaseTests(DatabaseFixture postgresFixture)
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
    }

    public async Task DisposeAsync() => await _dbContext.DisposeAsync();

    public void Dispose() => _dbContext?.Dispose();

    private ReleaseAccountBalanceConsumer CreateConsumer(
        out ITopicProducer<ReleaseAccountBalancePassed> passedProducer,
        out ITopicProducer<ReleaseAccountBalanceFailed> failedProducer,
        out ITopicProducer<ReleaseAccountBalanceSkipped> skippedProducer)
    {
        passedProducer = Substitute.For<ITopicProducer<ReleaseAccountBalancePassed>>();
        failedProducer = Substitute.For<ITopicProducer<ReleaseAccountBalanceFailed>>();
        skippedProducer = Substitute.For<ITopicProducer<ReleaseAccountBalanceSkipped>>();

        return new ReleaseAccountBalanceConsumer(
            NullLogger<ReleaseAccountBalanceConsumer>.Instance,
            _dbContext,
            passedProducer,
            failedProducer,
            skippedProducer);
    }

    private static ConsumeContext<ReleaseAccountBalance> BuildContext(ReleaseAccountBalance message)
    {
        ConsumeContext<ReleaseAccountBalance> context = Substitute.For<ConsumeContext<ReleaseAccountBalance>>();
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

    private async Task SeedHoldEntryAsync(Guid transactionId, Guid sourceAccountId, Guid targetAccountId, decimal amount)
    {
        await using NpgsqlConnection connection = new(_postgresFixture.Container.GetConnectionString());
        await connection.OpenAsync();
        await connection.ExecuteAsync($"""
            INSERT INTO "{nameof(BalanceDbContext.JournalEntries)}"
            ("{nameof(AccountJournalEntryEntity.SourceAccountId)}", "{nameof(AccountJournalEntryEntity.TargetAccountId)}", "{nameof(AccountJournalEntryEntity.SequenceNumber)}", "{nameof(AccountJournalEntryEntity.AmountDelta)}", "{nameof(AccountJournalEntryEntity.EntryType)}", "{nameof(AccountJournalEntryEntity.TransactionId)}", "{nameof(AccountJournalEntryEntity.TimestampUtc)}")
            VALUES (@SourceAccountId, @TargetAccountId, 1, @AmountDelta, @EntryType, @TransactionId, @TimestampUtc)
            """,
            new { SourceAccountId = sourceAccountId, TargetAccountId = targetAccountId, AmountDelta = -amount, EntryType = (int)JournalEntryType.Hold, TransactionId = transactionId, TimestampUtc = DateTime.UtcNow });
    }

    private async Task<long> CountCancelledEntriesAsync(Guid transactionId)
    {
        await using NpgsqlConnection connection = new(_postgresFixture.Container.GetConnectionString());
        await connection.OpenAsync();
        return await connection.ExecuteScalarAsync<long>(
            $"""SELECT count(*) FROM "{nameof(BalanceDbContext.JournalEntries)}" WHERE "{nameof(AccountJournalEntryEntity.TransactionId)}" = @TransactionId AND "{nameof(AccountJournalEntryEntity.EntryType)}" = @EntryType""",
            new { TransactionId = transactionId, EntryType = (int)JournalEntryType.Cancelled });
    }

    [Fact]
    public async Task Consume_WithRedeliveredDuplicateMessage_ShouldAppendTheCompensatingCreditExactlyOnce()
    {
        Guid sourceAccountId = Guid.NewGuid();
        Guid targetAccountId = Guid.NewGuid();
        Guid transactionId = Guid.NewGuid();

        await SeedAccountAsync(sourceAccountId, Currency.USD);
        await SeedAccountAsync(targetAccountId, Currency.USD);
        await SeedHoldEntryAsync(transactionId, sourceAccountId, targetAccountId, 100m);

        ReleaseAccountBalance message = new()
        {
            TransactionId = transactionId,
            AccountId = sourceAccountId,
            Amount = 100m,
            Reason = ReleaseAccountBalanceReason.ComplianceViolation
        };

        ReleaseAccountBalanceConsumer consumer = CreateConsumer(
            out ITopicProducer<ReleaseAccountBalancePassed> passedProducer, out _, out _);

        await consumer.Consume(BuildContext(message));
        await consumer.Consume(BuildContext(message));

        long cancelledCount = await CountCancelledEntriesAsync(transactionId);

        cancelledCount.Should().Be(1,
            "because a Kafka at-least-once redelivery of the exact same release must be replayed idempotently, not restore the held capacity twice");

        await passedProducer.Received(2).Produce(
            Arg.Is<ReleaseAccountBalancePassed>(e => e.TransactionId == transactionId),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Consume_WithNoMatchingHoldEntry_ShouldSkipCompensationWithoutFaulting()
    {
        Guid sourceAccountId = Guid.NewGuid();
        Guid transactionId = Guid.NewGuid();

        await SeedAccountAsync(sourceAccountId, Currency.USD);

        ReleaseAccountBalance message = new()
        {
            TransactionId = transactionId,
            AccountId = sourceAccountId,
            Amount = 50m,
            Reason = ReleaseAccountBalanceReason.LedgerWriteFailure
        };

        ReleaseAccountBalanceConsumer consumer = CreateConsumer(
            out _, out ITopicProducer<ReleaseAccountBalanceFailed> failedProducer, out ITopicProducer<ReleaseAccountBalanceSkipped> skippedProducer);

        await consumer.Consume(BuildContext(message));

        await skippedProducer.Received(1).Produce(
            Arg.Is<ReleaseAccountBalanceSkipped>(e => e.TransactionId == transactionId),
            Arg.Any<CancellationToken>());
        await failedProducer.DidNotReceive().Produce(Arg.Any<ReleaseAccountBalanceFailed>(), Arg.Any<CancellationToken>());
    }
}
