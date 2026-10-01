using FluentAssertions;
using IntegrationBus.CoreLedger.Service.Activities;
using IntegrationBus.CoreLedger.Service.Models;
using MassTransit;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using StackExchange.Redis;
using Testcontainers.Redis;

namespace IntegrationBus.CoreLedger.Service.Tests.Activities;

/// <summary>
/// Exercises <see cref="UpdateCacheActivity"/> -- the second stage of the Core Ledger Courier routing slip -- against
/// a real Redis (Testcontainers) so the actual StackExchange.Redis SETEX/DEL code paths run exactly as in production.
/// </summary>
public sealed class UpdateCacheActivityTests : IAsyncLifetime
{
    private readonly RedisContainer _redis = new RedisBuilder("redis:7-alpine").Build();
    private IConnectionMultiplexer _multiplexer = null!;

    private static string CacheKey(Guid transactionId) => $"ledger:tx:{transactionId}";

    public async Task InitializeAsync()
    {
        await _redis.StartAsync();
        _multiplexer = await ConnectionMultiplexer.ConnectAsync(_redis.GetConnectionString());
    }

    public async Task DisposeAsync()
    {
        await _multiplexer.DisposeAsync();
        await _redis.DisposeAsync();
    }

    [Fact]
    public async Task Execute_ShouldWriteTheTransactionAmountIntoRedisUnderTheExpectedKey()
    {
        Guid transactionId = Guid.NewGuid();
        UpdateCacheArguments arguments = new() { TransactionId = transactionId, Amount = 123.45m };

        ExecuteContext<UpdateCacheArguments> context = Substitute.For<ExecuteContext<UpdateCacheArguments>>();
        context.Arguments.Returns(arguments);
        context.CancellationToken.Returns(CancellationToken.None);

        ExecutionResult expectedResult = Substitute.For<ExecutionResult>();
        context.Completed(Arg.Any<UpdateCacheLog>()).Returns(expectedResult);

        UpdateCacheActivity activity = new(NullLogger<UpdateCacheActivity>.Instance, _multiplexer);

        ExecutionResult actualResult = await activity.Execute(context);

        actualResult.Should().BeSameAs(expectedResult);

        IDatabase database = _multiplexer.GetDatabase();
        RedisValue cachedValue = await database.StringGetAsync(CacheKey(transactionId));
        cachedValue.HasValue.Should().BeTrue("because Execute must persist the committed amount into Redis so downstream reads avoid a database round-trip");
        decimal.Parse((string)cachedValue!, System.Globalization.CultureInfo.InvariantCulture).Should().Be(123.45m);

        context.Received(1).Completed(Arg.Is<UpdateCacheLog>(log => log.TransactionId == transactionId));
    }

    [Fact]
    public async Task Compensate_ShouldEvictThePreviouslyCachedEntry()
    {
        Guid transactionId = Guid.NewGuid();
        IDatabase database = _multiplexer.GetDatabase();
        await database.StringSetAsync(CacheKey(transactionId), "999.00");

        UpdateCacheLog log = new() { TransactionId = transactionId };
        CompensateContext<UpdateCacheLog> context = Substitute.For<CompensateContext<UpdateCacheLog>>();
        context.Log.Returns(log);

        CompensationResult expectedResult = Substitute.For<CompensationResult>();
        context.Compensated().Returns(expectedResult);

        UpdateCacheActivity activity = new(NullLogger<UpdateCacheActivity>.Instance, _multiplexer);

        CompensationResult actualResult = await activity.Compensate(context);

        actualResult.Should().BeSameAs(expectedResult);

        bool keyStillExists = await database.KeyExistsAsync(CacheKey(transactionId));
        keyStillExists.Should().BeFalse("because compensating a failed downstream routing slip step must roll back the cache mutation");
    }
}
