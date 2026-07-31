using System.Data.Common;
using Dapper;
using FluentAssertions;
using IntegrationBus.AccountBalance.Service.DbContexts;
using IntegrationBus.AccountBalance.Service.Entities;
using IntegrationBus.AccountBalance.Service.Providers;
using IntegrationBus.AccountBalance.Service.Tests.Fixtures;
using Npgsql;

namespace IntegrationBus.AccountBalance.Service.Tests.Providers;

/// <summary>
/// Provides isolated database integration validation layers confirming net state calculation behaviors.
/// </summary>
public sealed class AccountStateReconstructorTests(DatabaseFixture fixture) :
    IClassFixture<DatabaseFixture>,
    IAsyncLifetime,
    IDisposable
{
    private DbConnection _connection = null!;
    private readonly AccountStateReconstructor _reconstructor = new();

    /// <inheritdoc />
    public async Task InitializeAsync()
    {
        _connection = new NpgsqlConnection(fixture.Container.GetConnectionString());
        await _connection.OpenAsync();

        // 1. Create the immutable journal entries tracking storage log layout
        await _connection.ExecuteAsync($@"
            CREATE TABLE IF NOT EXISTS ""{nameof(BalanceDbContext.JournalEntries)}"" (
                ""{nameof(AccountJournalEntryEntity.SourceAccountId)}"" UUID NOT NULL,
                ""{nameof(AccountJournalEntryEntity.TargetAccountId)}"" UUID,
                ""{nameof(AccountJournalEntryEntity.SequenceNumber)}"" BIGINT NOT NULL,
                ""{nameof(AccountJournalEntryEntity.AmountDelta)}"" NUMERIC(18,2) NOT NULL,
                ""{nameof(AccountJournalEntryEntity.EntryType)}"" INT NOT NULL,
                ""{nameof(AccountJournalEntryEntity.TransactionId)}"" UUID NOT NULL,
                ""{nameof(AccountJournalEntryEntity.TimestampUtc)}"" TIMESTAMP WITH TIME ZONE NOT NULL,
                PRIMARY KEY (""{nameof(AccountJournalEntryEntity.SourceAccountId)}"", ""{nameof(AccountJournalEntryEntity.SequenceNumber)}"")
            );");

        // 2. Create the absolute checkpoint snapshots metadata cache layout
        await _connection.ExecuteAsync($@"
            CREATE TABLE IF NOT EXISTS ""{nameof(BalanceDbContext.Snapshots)}"" (
                ""{nameof(AccountSnapshotEntity.AccountId)}"" UUID NOT NULL,
                ""{nameof(AccountSnapshotEntity.SequenceNumber)}"" BIGINT NOT NULL,
                ""{nameof(AccountSnapshotEntity.SnapshotBalance)}"" NUMERIC(18,2) NOT NULL,
                ""{nameof(AccountSnapshotEntity.CapturedAtUtc)}"" TIMESTAMP WITH TIME ZONE NOT NULL,
                PRIMARY KEY (""{nameof(AccountSnapshotEntity.AccountId)}"", ""{nameof(AccountSnapshotEntity.SequenceNumber)}"")
            );");
    }

    /// <inheritdoc />
    public async Task DisposeAsync()
    {
        if (_connection is not null)
        {
            await _connection.DisposeAsync();
        }
    }

    /// <summary>
    /// Implements synchronous disposal routine chains to satisfy diagnostic static analyzer infrastructure rules.
    /// </summary>
    public void Dispose()
    {
        _connection?.Dispose();
    }

    [Fact]
    public async Task ReconstructAvailableBalanceAsync_WithNoHistory_ShouldReturnZeroBalance()
    {
        // Arrange
        Guid accountId = Guid.NewGuid();
        using DbTransaction transaction = await _connection.BeginTransactionAsync();

        // Act
        decimal actualBalance = await _reconstructor.ReconstructAvailableBalanceAsync(accountId, _connection, transaction);

        // Assert
        actualBalance.Should().Be(0.00m);
        await transaction.RollbackAsync();
    }

    [Fact]
    public async Task ReconstructAvailableBalanceAsync_WithOnlySnapshot_ShouldReturnSnapshotBalanceValue()
    {
        // Arrange
        Guid accountId = Guid.NewGuid();
        decimal expectedSnapshotBalance = 1500.75m;
        using DbTransaction transaction = await _connection.BeginTransactionAsync();

        await _connection.ExecuteAsync($@"
            INSERT INTO ""{nameof(BalanceDbContext.Snapshots)}"" 
            (""{nameof(AccountSnapshotEntity.AccountId)}"", ""{nameof(AccountSnapshotEntity.SequenceNumber)}"", ""{nameof(AccountSnapshotEntity.SnapshotBalance)}"", ""{nameof(AccountSnapshotEntity.CapturedAtUtc)}"")
            VALUES (@AccountId, @SequenceNumber, @SnapshotBalance, @TimestampUtc);",
            new { AccountId = accountId, SequenceNumber = 45L, SnapshotBalance = expectedSnapshotBalance, TimestampUtc = DateTime.UtcNow },
            transaction);

        // Act
        decimal actualBalance = await _reconstructor.ReconstructAvailableBalanceAsync(accountId, _connection, transaction);

        // Assert
        actualBalance.Should().Be(expectedSnapshotBalance);
        await transaction.RollbackAsync();
    }

    [Fact]
    public async Task ReconstructAvailableBalanceAsync_WithSnapshotAndUnsavedJournalDeltas_ShouldCorrectlyAggregateNetBalanceState()
    {
        // Arrange
        Guid accountId = Guid.NewGuid();
        decimal snapshotBaseBalance = 500.00m;
        using DbTransaction transaction = await _connection.BeginTransactionAsync();

        await _connection.ExecuteAsync($@"
            INSERT INTO ""{nameof(BalanceDbContext.Snapshots)}"" 
            (""{nameof(AccountSnapshotEntity.AccountId)}"", ""{nameof(AccountSnapshotEntity.SequenceNumber)}"", ""{nameof(AccountSnapshotEntity.SnapshotBalance)}"", ""{nameof(AccountSnapshotEntity.CapturedAtUtc)}"")
            VALUES (@AccountId, @SequenceNumber, @SnapshotBalance, @TimestampUtc);",
            new { AccountId = accountId, SequenceNumber = 10L, SnapshotBalance = snapshotBaseBalance, TimestampUtc = DateTime.UtcNow },
            transaction);

        await InsertTestJournalEntry(accountId, sequenceNumber: 8L, amountDelta: -100.00m, transaction);
        await InsertTestJournalEntry(accountId, sequenceNumber: 11L, amountDelta: -150.00m, transaction);
        await InsertTestJournalEntry(accountId, sequenceNumber: 12L, amountDelta: 300.50m, transaction);
        await InsertTestJournalEntry(accountId, sequenceNumber: 13L, amountDelta: -25.00m, transaction);

        decimal expectedFinalBalance = 625.50m;

        // Act
        decimal actualBalance = await _reconstructor.ReconstructAvailableBalanceAsync(accountId, _connection, transaction);

        // Assert
        actualBalance.Should().Be(expectedFinalBalance);
        await transaction.RollbackAsync();
    }

    private async Task InsertTestJournalEntry(Guid accountId, long sequenceNumber, decimal amountDelta, DbTransaction transaction)
    {
        await _connection.ExecuteAsync($@"
            INSERT INTO ""{nameof(BalanceDbContext.JournalEntries)}"" (
                ""{nameof(AccountJournalEntryEntity.SourceAccountId)}"", 
                ""{nameof(AccountJournalEntryEntity.SequenceNumber)}"", 
                ""{nameof(AccountJournalEntryEntity.AmountDelta)}"", 
                ""{nameof(AccountJournalEntryEntity.EntryType)}"", 
                ""{nameof(AccountJournalEntryEntity.TransactionId)}"", 
                ""{nameof(AccountJournalEntryEntity.TimestampUtc)}"")
            VALUES (@SourceAccountId, @SequenceNumber, @AmountDelta, 1, @TransactionId, @TimestampUtc);",
            new
            {
                SourceAccountId = accountId,
                SequenceNumber = sequenceNumber,
                AmountDelta = amountDelta,
                TransactionId = Guid.NewGuid(),
                TimestampUtc = DateTime.UtcNow
            }, transaction);
    }
}
