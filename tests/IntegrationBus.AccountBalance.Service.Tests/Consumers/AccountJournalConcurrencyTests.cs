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
/// Verifies against a real Postgres that concurrent consumers (standing in for several service replicas) appending to
/// the same account journal never collide on the sequence number and never double-post a redelivered transaction.
/// Locking design: <c>docs/chaos-engineering/README.md</c> (relative to the repository root).
/// </summary>
public sealed class AccountJournalConcurrencyTests(DatabaseFixture postgres) : IClassFixture<DatabaseFixture>, IAsyncLifetime
{
    private const int ConcurrentTopUps = 24;

    public async Task InitializeAsync()
    {
        await using BalanceDbContext dbContext = CreateDbContext();
        await dbContext.Database.EnsureCreatedAsync();
    }

    public Task DisposeAsync() => Task.CompletedTask;

    private BalanceDbContext CreateDbContext() => new(
        new DbContextOptionsBuilder<BalanceDbContext>().UseNpgsql(postgres.Container.GetConnectionString()).Options);

    private static ConsumeContext<TopUpAccountBalance> BuildContext(TopUpAccountBalance message)
    {
        ConsumeContext<TopUpAccountBalance> context = Substitute.For<ConsumeContext<TopUpAccountBalance>>();
        context.Message.Returns(message);
        context.CancellationToken.Returns(CancellationToken.None);
        return context;
    }

    [Fact]
    public async Task Consume_WithConcurrentTopUpsOnOneAccount_ShouldAssignUniqueSequencesWithoutFailures()
    {
        Guid accountId = Guid.NewGuid();
        await using (NpgsqlConnection seed = new(postgres.Container.GetConnectionString()))
        {
            await seed.OpenAsync();
            await seed.ExecuteAsync(
                $"""INSERT INTO "{nameof(BalanceDbContext.Accounts)}" ("{nameof(AccountEntity.Id)}", "{nameof(AccountEntity.Currency)}", "{nameof(AccountEntity.CreatedAt)}") VALUES (@Id, @Currency, @CreatedAt)""",
                new { Id = accountId, Currency = (int)Currency.USD, CreatedAt = DateTime.UtcNow });
        }

        ITopicProducer<TopUpAccountBalanceFailed> failedProducer = Substitute.For<ITopicProducer<TopUpAccountBalanceFailed>>();
        Guid duplicatedTransactionId = Guid.NewGuid();

        // One extra pair of deliveries shares a TransactionId to emulate a Kafka redelivery racing the original
        List<Guid> transactionIds = [.. Enumerable.Range(0, ConcurrentTopUps).Select(_ => Guid.NewGuid()), duplicatedTransactionId, duplicatedTransactionId];

        await Task.WhenAll(transactionIds.Select(async transactionId =>
        {
            await using BalanceDbContext dbContext = CreateDbContext();
            TopUpAccountBalanceConsumer consumer = new(
                dbContext,
                NullLogger<TopUpAccountBalanceConsumer>.Instance,
                Substitute.For<ITopicProducer<TopUpAccountBalancePassed>>(),
                failedProducer);

            await consumer.Consume(BuildContext(new TopUpAccountBalance
            {
                TransactionId = transactionId,
                AccountId = accountId,
                Amount = 10m,
                Currency = Currency.USD
            }));
        }));

        await failedProducer.DidNotReceive().Produce(Arg.Any<TopUpAccountBalanceFailed>(), Arg.Any<CancellationToken>());

        await using NpgsqlConnection connection = new(postgres.Container.GetConnectionString());
        await connection.OpenAsync();
        List<long> sequences = (await connection.QueryAsync<long>(
            $"""SELECT "{nameof(AccountJournalEntryEntity.SequenceNumber)}" FROM "{nameof(BalanceDbContext.JournalEntries)}" WHERE "{nameof(AccountJournalEntryEntity.SourceAccountId)}" = @AccountId AND "{nameof(AccountJournalEntryEntity.EntryType)}" = @EntryType ORDER BY 1""",
            new { AccountId = accountId, EntryType = (int)JournalEntryType.DirectDeposit })).ToList();

        sequences
            .Should()
            .Equal(
                Enumerable.Range(1, ConcurrentTopUps + 1).Select(sequence => (long)sequence),
                "because every distinct transaction must receive its own gapless sequence number and the duplicated transaction must be posted once");
    }
}
