using System.Collections.Concurrent;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using FluentAssertions;
using IntegrationBus.Shared.Extensions;
using MassTransit;
using Microsoft.Extensions.DependencyInjection;
using Testcontainers.Kafka;

namespace IntegrationBus.Shared.Tests.Extensions;

/// <summary>
/// Verifies partition key routing of <c>AddPartitionKeyedProducer</c> against a real Kafka broker.
/// Scenario description: <c>docs/scaling/kafka-partition-keys.md</c> (relative to the repository root).
/// </summary>
public sealed class KafkaPartitionKeyRoutingTests : IAsyncLifetime
{
    private const string Topic = "partition-key-routing-test";
    private const string ConsumerGroup = "partition-key-routing-group";
    private const int PartitionCount = 6;
    private const int AccountCount = 24;
    private const int OperationsPerAccount = 15;

    private readonly KafkaContainer _kafka = new KafkaBuilder("confluentinc/cp-kafka:7.5.12").Build();

    public sealed record AccountOperation
    {
        public Guid AccountId { get; init; }

        public int Sequence { get; init; }
    }

    public sealed class ReplicaRecorder(string replicaName, ConcurrentQueue<(string Replica, Guid AccountId, int Sequence)> log)
        : IConsumer<AccountOperation>
    {
        public Task Consume(ConsumeContext<AccountOperation> context)
        {
            log.Enqueue((replicaName, context.Message.AccountId, context.Message.Sequence));
            return Task.CompletedTask;
        }
    }

    public async Task InitializeAsync()
    {
        await _kafka.StartAsync();

        using IAdminClient admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = _kafka.GetBootstrapAddress() }).Build();
        await admin.CreateTopicsAsync([new TopicSpecification { Name = Topic, NumPartitions = PartitionCount, ReplicationFactor = 1 }]);
    }

    public async Task DisposeAsync() => await _kafka.DisposeAsync();

    [Fact]
    public async Task Produce_WithPartitionKeyedProducer_ShouldWriteAccountIdAsKeyAndKeepEachAccountOnOnePartition()
    {
        List<Guid> accounts = Enumerable.Range(0, AccountCount).Select(_ => Guid.NewGuid()).ToList();

        await using ServiceProvider producerHost = BuildProducerHost();
        IBusControl bus = producerHost.GetRequiredService<IBusControl>();
        await bus.StartAsync();

        try
        {
            await ProduceInterleavedOperationsAsync(producerHost, accounts);
        }
        finally
        {
            await bus.StopAsync();
        }

        List<ConsumeResult<string, byte[]>> records = ReadAllRecordsRaw(expectedCount: AccountCount * OperationsPerAccount);

        records
            .Should()
            .HaveCount(AccountCount * OperationsPerAccount, "because every produced operation must be readable from the topic")
            .And.OnlyContain(
                record => record.Message.Key != null,
                "because a keyless record would be spread across partitions and lose per-account ordering");

        foreach (IGrouping<string, ConsumeResult<string, byte[]>> accountRecords in records.GroupBy(record => record.Message.Key))
        {
            accountRecords
                .Select(record => record.Partition.Value)
                .Distinct()
                .Should()
                .ContainSingle("because Kafka hashes the record key to a single partition, so one account never spans partitions");
        }

        records
            .Select(record => record.Message.Key)
            .Distinct()
            .Should()
            .BeEquivalentTo(
                accounts.Select(KafkaPartitionKeyExtensions.ToPartitionKey),
                "because the record key must be the canonical string form of the account identifier");

        records
            .Select(record => record.Partition.Value)
            .Distinct()
            .Count()
            .Should()
            .BeGreaterThan(1, "because distinct accounts must still be distributed across partitions to scale horizontally");
    }

    [Fact]
    public async Task Consume_WithTwoReplicasInOneGroup_ShouldProcessEachAccountOnOneReplicaInProductionOrder()
    {
        List<Guid> accounts = Enumerable.Range(0, AccountCount).Select(_ => Guid.NewGuid()).ToList();
        ConcurrentQueue<(string Replica, Guid AccountId, int Sequence)> log = new();

        await using ServiceProvider replicaOne = BuildConsumerHost("replica-1", log);
        await using ServiceProvider replicaTwo = BuildConsumerHost("replica-2", log);
        await using ServiceProvider producerHost = BuildProducerHost();

        IBusControl busOne = replicaOne.GetRequiredService<IBusControl>();
        IBusControl busTwo = replicaTwo.GetRequiredService<IBusControl>();
        IBusControl producerBus = producerHost.GetRequiredService<IBusControl>();

        await busOne.StartAsync();
        await busTwo.StartAsync();
        await producerBus.StartAsync();

        try
        {
            await ProduceInterleavedOperationsAsync(producerHost, accounts);

            int expectedTotal = AccountCount * OperationsPerAccount;
            DateTime deadline = DateTime.UtcNow.AddSeconds(90);
            while (log.Count < expectedTotal && DateTime.UtcNow < deadline)
            {
                await Task.Delay(250);
            }
        }
        finally
        {
            await producerBus.StopAsync();
            await busOne.StopAsync();
            await busTwo.StopAsync();
        }

        List<(string Replica, Guid AccountId, int Sequence)> processed = [.. log];

        processed
            .Should()
            .HaveCount(AccountCount * OperationsPerAccount, "because every operation must be processed exactly once across the replicas");

        foreach (IGrouping<Guid, (string Replica, Guid AccountId, int Sequence)> accountLog in processed.GroupBy(entry => entry.AccountId))
        {
            accountLog
                .Select(entry => entry.Replica)
                .Distinct()
                .Should()
                .ContainSingle("because all operations of one account share a partition owned by exactly one replica");

            accountLog
                .Select(entry => entry.Sequence)
                .Should()
                .Equal(
                    Enumerable.Range(1, OperationsPerAccount),
                    "because a single partition is consumed strictly in the order the operations were produced");
        }

        processed
            .Select(entry => entry.Replica)
            .Distinct()
            .Should()
            .HaveCount(2, "because the six partitions must be shared between both replicas of the consumer group");
    }

    private ServiceProvider BuildProducerHost()
    {
        ServiceCollection services = new();
        services.AddMassTransit(x =>
        {
            x.UsingInMemory();
            x.AddRider(rider =>
            {
                rider.AddPartitionKeyedProducer<AccountOperation>(Topic, message => message.AccountId);
                rider.UsingKafka((_, k) => k.Host(_kafka.GetBootstrapAddress()));
            });
        });

        return services.BuildServiceProvider();
    }

    private ServiceProvider BuildConsumerHost(string replicaName, ConcurrentQueue<(string Replica, Guid AccountId, int Sequence)> log)
    {
        ServiceCollection services = new();
        services.AddSingleton(log);
        services.AddScoped(_ => new ReplicaRecorder(replicaName, log));
        services.AddMassTransit(x =>
        {
            x.UsingInMemory();
            x.AddRider(rider =>
            {
                rider.AddConsumer<ReplicaRecorder>();
                rider.UsingKafka((context, k) =>
                {
                    k.Host(_kafka.GetBootstrapAddress());
                    k.TopicEndpoint<AccountOperation>(Topic, ConsumerGroup, e =>
                    {
                        e.AutoOffsetReset = AutoOffsetReset.Earliest;
                        e.ConfigureConsumer<ReplicaRecorder>(context);
                    });
                });
            });
        });

        return services.BuildServiceProvider();
    }

    private static async Task ProduceInterleavedOperationsAsync(ServiceProvider producerHost, List<Guid> accounts)
    {
        ITopicProducer<AccountOperation> producer = producerHost.GetRequiredService<ITopicProducer<AccountOperation>>();

        // Operations of different accounts are interleaved so ordering can only hold through key-based routing
        for (int sequence = 1; sequence <= OperationsPerAccount; sequence++)
        {
            foreach (Guid account in accounts)
            {
                await producer.Produce(new AccountOperation { AccountId = account, Sequence = sequence });
            }
        }
    }

    private List<ConsumeResult<string, byte[]>> ReadAllRecordsRaw(int expectedCount)
    {
        ConsumerConfig config = new()
        {
            BootstrapServers = _kafka.GetBootstrapAddress(),
            GroupId = $"raw-reader-{Guid.NewGuid()}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using IConsumer<string, byte[]> consumer = new ConsumerBuilder<string, byte[]>(config)
            .SetKeyDeserializer(new Utf8KeyDeserializer())
            .Build();
        consumer.Subscribe(Topic);

        List<ConsumeResult<string, byte[]>> records = [];
        DateTime deadline = DateTime.UtcNow.AddSeconds(60);
        while (records.Count < expectedCount && DateTime.UtcNow < deadline)
        {
            ConsumeResult<string, byte[]>? result = consumer.Consume(TimeSpan.FromSeconds(1));
            if (result is not null)
            {
                records.Add(result);
            }
        }

        return records;
    }

    private sealed class Utf8KeyDeserializer : IDeserializer<string>
    {
        public string Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) =>
            isNull ? null! : Encoding.UTF8.GetString(data);
    }
}
