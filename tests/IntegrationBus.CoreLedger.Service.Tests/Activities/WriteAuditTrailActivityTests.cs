using Dapper;
using FluentAssertions;
using IntegrationBus.CoreLedger.Service.Activities;
using IntegrationBus.CoreLedger.Service.DbContexts;
using IntegrationBus.CoreLedger.Service.Entities;
using IntegrationBus.CoreLedger.Service.Models;
using MassTransit;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NSubstitute;
using Testcontainers.PostgreSql;

namespace IntegrationBus.CoreLedger.Service.Tests.Activities;

/// <summary>
/// Exercises <see cref="WriteAuditTrailActivity"/> -- the first stage of the Core Ledger Courier routing slip --
/// against a real Postgres (Testcontainers) so the actual Npgsql/Dapper INSERT runs exactly as in production, not
/// just the in-memory control flow.
/// </summary>
public sealed class WriteAuditTrailActivityTests : IAsyncLifetime, IDisposable
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("postgres:16")
        .WithDatabase("core_ledger_test")
        .WithUsername("postgres")
        .WithPassword("postgres")
        .Build();

    private LedgerDbContext _dbContext = null!;

    public async Task InitializeAsync()
    {
        await _postgres.StartAsync();

        DbContextOptions<LedgerDbContext> options = new DbContextOptionsBuilder<LedgerDbContext>()
            .UseNpgsql(_postgres.GetConnectionString())
            .Options;
        _dbContext = new LedgerDbContext(options);

        await _dbContext.Database.MigrateAsync();
    }

    public async Task DisposeAsync()
    {
        await _dbContext.DisposeAsync();
        await _postgres.DisposeAsync();
    }

    public void Dispose() => _dbContext?.Dispose();

    private async Task<LedgerEntryEntity?> FindEntryAsync(Guid transactionId)
    {
        await using NpgsqlConnection connection = new(_postgres.GetConnectionString());
        await connection.OpenAsync();
        return await connection.QuerySingleOrDefaultAsync<LedgerEntryEntity>(
            $"""SELECT * FROM "{nameof(LedgerDbContext.LedgerEntries)}" WHERE "{nameof(LedgerEntryEntity.TransactionId)}" = @TransactionId""",
            new { TransactionId = transactionId });
    }

    [Fact]
    public async Task Execute_ShouldAppendTheJournalRecordAndCompleteWithTheGeneratedEntryId()
    {
        Guid transactionId = Guid.NewGuid();
        WriteAuditTrailArguments arguments = new()
        {
            TransactionId = transactionId,
            SourceAccountId = Guid.NewGuid(),
            TargetAccountId = Guid.NewGuid(),
            Amount = 250m,
            Currency = (int)IntegrationBus.Contracts.Enums.Currency.USD
        };

        ExecuteContext<WriteAuditTrailArguments> context = Substitute.For<ExecuteContext<WriteAuditTrailArguments>>();
        context.Arguments.Returns(arguments);
        context.CancellationToken.Returns(CancellationToken.None);

        ExecutionResult expectedResult = Substitute.For<ExecutionResult>();
        context.Completed(Arg.Any<WriteAuditTrailLog>()).Returns(expectedResult);

        WriteAuditTrailActivity activity = new(NullLogger<WriteAuditTrailActivity>.Instance, _dbContext);

        ExecutionResult actualResult = await activity.Execute(context);

        actualResult.Should().BeSameAs(expectedResult, "because a successful execution must return exactly what context.Completed(...) produced");

        LedgerEntryEntity? persisted = await FindEntryAsync(transactionId);
        persisted.Should().NotBeNull("because Execute must append a real row into the ledger journal table");
        persisted!.Amount.Should().Be(250m);

        context.Received(1).Completed(Arg.Is<WriteAuditTrailLog>(log =>
            log.TransactionId == transactionId && log.LedgerEntryId > 0));
    }

    [Fact]
    public async Task Execute_WhenTheDatabaseIsUnreachable_ShouldFaultInsteadOfThrowing()
    {
        await _postgres.StopAsync();

        WriteAuditTrailArguments arguments = new()
        {
            TransactionId = Guid.NewGuid(),
            SourceAccountId = Guid.NewGuid(),
            TargetAccountId = Guid.NewGuid(),
            Amount = 10m,
            Currency = (int)IntegrationBus.Contracts.Enums.Currency.USD
        };

        ExecuteContext<WriteAuditTrailArguments> context = Substitute.For<ExecuteContext<WriteAuditTrailArguments>>();
        context.Arguments.Returns(arguments);
        context.CancellationToken.Returns(CancellationToken.None);

        ExecutionResult expectedFaultResult = Substitute.For<ExecutionResult>();
        context.Faulted(Arg.Any<Exception>()).Returns(expectedFaultResult);

        WriteAuditTrailActivity activity = new(NullLogger<WriteAuditTrailActivity>.Instance, _dbContext);

        ExecutionResult actualResult = await activity.Execute(context);

        actualResult.Should().BeSameAs(expectedFaultResult,
            "because the activity must translate an infrastructure exception into context.Faulted(...) rather than letting it propagate");
    }

    [Fact]
    public async Task Compensate_ShouldReportCompensatedWithoutThrowing()
    {
        WriteAuditTrailLog log = new() { TransactionId = Guid.NewGuid(), LedgerEntryId = 42 };

        CompensateContext<WriteAuditTrailLog> context = Substitute.For<CompensateContext<WriteAuditTrailLog>>();
        context.Log.Returns(log);

        CompensationResult expectedResult = Substitute.For<CompensationResult>();
        context.Compensated().Returns(expectedResult);

        WriteAuditTrailActivity activity = new(NullLogger<WriteAuditTrailActivity>.Instance, _dbContext);

        CompensationResult actualResult = await activity.Compensate(context);

        actualResult.Should().BeSameAs(expectedResult);
        context.Received(1).Compensated();
    }

    [Fact]
    public async Task Execute_WhenTheSameTransactionIsDeliveredTwice_ShouldKeepExactlyOneLedgerEntry()
    {
        Guid transactionId = Guid.NewGuid();
        WriteAuditTrailActivity activity = new(NullLogger<WriteAuditTrailActivity>.Instance, _dbContext);

        for (int delivery = 0; delivery < 2; delivery++)
        {
            ExecuteContext<WriteAuditTrailArguments> context = Substitute.For<ExecuteContext<WriteAuditTrailArguments>>();
            context.Arguments.Returns(new WriteAuditTrailArguments
            {
                TransactionId = transactionId,
                SourceAccountId = Guid.NewGuid(),
                TargetAccountId = Guid.NewGuid(),
                Amount = 75m,
                Currency = (int)IntegrationBus.Contracts.Enums.Currency.USD
            });
            context.CancellationToken.Returns(CancellationToken.None);

            await activity.Execute(context);
        }

        await using NpgsqlConnection connection = new(_postgres.GetConnectionString());
        await connection.OpenAsync();
        int rowCount = await connection.ExecuteScalarAsync<int>(
            $"""SELECT COUNT(*) FROM "{nameof(LedgerDbContext.LedgerEntries)}" WHERE "{nameof(LedgerEntryEntity.TransactionId)}" = @TransactionId""",
            new { TransactionId = transactionId });

        rowCount.Should().Be(1, "because a redelivered WriteLedgerRecord command must not duplicate the financial record");
    }

    [Fact]
    public async Task Compensate_ShouldRemoveTheLedgerEntryOfTheRolledBackTransaction()
    {
        Guid transactionId = Guid.NewGuid();
        WriteAuditTrailActivity activity = new(NullLogger<WriteAuditTrailActivity>.Instance, _dbContext);

        ExecuteContext<WriteAuditTrailArguments> executeContext = Substitute.For<ExecuteContext<WriteAuditTrailArguments>>();
        executeContext.Arguments.Returns(new WriteAuditTrailArguments
        {
            TransactionId = transactionId,
            SourceAccountId = Guid.NewGuid(),
            TargetAccountId = Guid.NewGuid(),
            Amount = 30m,
            Currency = (int)IntegrationBus.Contracts.Enums.Currency.USD
        });
        executeContext.CancellationToken.Returns(CancellationToken.None);
        await activity.Execute(executeContext);

        CompensateContext<WriteAuditTrailLog> compensateContext = Substitute.For<CompensateContext<WriteAuditTrailLog>>();
        compensateContext.Log.Returns(new WriteAuditTrailLog { TransactionId = transactionId, LedgerEntryId = 1 });
        await activity.Compensate(compensateContext);

        LedgerEntryEntity? remaining = await FindEntryAsync(transactionId);
        remaining.Should().BeNull("because a rolled back routing slip must leave no ledger record for a failed transaction");
    }
}
