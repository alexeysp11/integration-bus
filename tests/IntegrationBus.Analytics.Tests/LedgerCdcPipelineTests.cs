using System.Net.Http.Json;
using ClickHouse.Client.ADO;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using FluentAssertions;
using Npgsql;
using Polly;
using Polly.Retry;
using Testcontainers.ClickHouse;
using Testcontainers.PostgreSql;

namespace IntegrationBus.Analytics.Tests;

/// <summary>
/// End-to-end proof of the real-time analytics pipeline: a row written to Postgres is captured by a Debezium
/// Postgres source connector running inside Kafka Connect, streamed onto a Kafka CDC topic, and ingested by
/// ClickHouse's native Kafka table engine into a queryable MergeTree table via a Materialized View -- with zero
/// application code involved, mirroring exactly the production wiring in <c>docker-compose.yml</c> and
/// <c>infrastructure/clickhouse/init.sql</c>.
/// </summary>
public sealed class LedgerCdcPipelineTests : IAsyncLifetime
{
    private const string DebeziumConnectImage = "debezium/connect:3.0.0.Final";
    private const string KafkaNetworkAlias = "kafka";
    private const string PostgresNetworkAlias = "postgres";
    private const int KafkaConnectPort = 8083;

    private INetwork _network = null!;
    private PostgreSqlContainer _postgres = null!;
    private IContainer _kafka = null!;
    private IContainer _kafkaConnect = null!;
    private ClickHouseContainer _clickHouse = null!;

    public async Task InitializeAsync()
    {
        _network = new NetworkBuilder().Build();

        _postgres = new PostgreSqlBuilder("postgres:16")
            .WithNetwork(_network)
            .WithNetworkAliases(PostgresNetworkAlias)
            .WithDatabase("ledger_db")
            .WithUsername("postgres")
            .WithPassword("postgres")
            .WithCommand("-c", "wal_level=logical", "-c", "max_wal_senders=10", "-c", "max_replication_slots=10")
            .Build();

        // A raw single-node KRaft broker configured identically to docker-compose.yml's integration-bus-kafka
        // service: it advertises itself under its network alias so sibling containers (Kafka Connect, ClickHouse)
        // can reach it, rather than relying on Testcontainers.Kafka's host-oriented default advertised listener
        // (which is unreachable from another container).
        _kafka = new ContainerBuilder("apache/kafka:3.9.0")
            .WithNetwork(_network)
            .WithNetworkAliases(KafkaNetworkAlias)
            .WithEnvironment("KAFKA_NODE_ID", "1")
            .WithEnvironment("KAFKA_PROCESS_ROLES", "broker,controller")
            .WithEnvironment("KAFKA_CONTROLLER_QUORUM_VOTERS", $"1@{KafkaNetworkAlias}:9093")
            .WithEnvironment("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
            .WithEnvironment("KAFKA_LISTENERS", "PLAINTEXT://:9094,CONTROLLER://:9093")
            .WithEnvironment("KAFKA_ADVERTISED_LISTENERS", $"PLAINTEXT://{KafkaNetworkAlias}:9094")
            .WithEnvironment("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT")
            .WithEnvironment("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
            .WithEnvironment("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
            .WithEnvironment("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
            .WithEnvironment("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
            .WithEnvironment("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilCommandIsCompleted(
                "/opt/kafka/bin/kafka-broker-api-versions.sh", "--bootstrap-server", $"{KafkaNetworkAlias}:9094"))
            .Build();

        // Pinned to the same version proven compatible with the Kafka broker in docker-compose.yml -- the
        // module's default bundled image (23.6.3) predates ClickHouse's system.kafka_consumers table and,
        // more importantly, its older librdkafka silently fails to consume from a modern Kafka broker.
        _clickHouse = new ClickHouseBuilder("clickhouse/clickhouse-server:24.8")
            .WithNetwork(_network)
            .WithNetworkAliases("clickhouse")
            .Build();

        await Task.WhenAll(
            _postgres.StartAsync(),
            _kafka.StartAsync(),
            _clickHouse.StartAsync());

        _kafkaConnect = new ContainerBuilder(DebeziumConnectImage)
            .WithNetwork(_network)
            .WithNetworkAliases("kafka-connect")
            .WithEnvironment("BOOTSTRAP_SERVERS", $"{KafkaNetworkAlias}:9094")
            .WithEnvironment("GROUP_ID", "analytics-test-connect")
            .WithEnvironment("CONFIG_STORAGE_TOPIC", "connect-configs")
            .WithEnvironment("OFFSET_STORAGE_TOPIC", "connect-offsets")
            .WithEnvironment("STATUS_STORAGE_TOPIC", "connect-status")
            .WithEnvironment("CONFIG_STORAGE_REPLICATION_FACTOR", "1")
            .WithEnvironment("OFFSET_STORAGE_REPLICATION_FACTOR", "1")
            .WithEnvironment("STATUS_STORAGE_REPLICATION_FACTOR", "1")
            .WithPortBinding(KafkaConnectPort, true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(request =>
                request.ForPort(KafkaConnectPort).ForPath("/connectors")))
            .Build();

        await _kafkaConnect.StartAsync();

        await CreateLedgerEntriesTableAsync();
        await RegisterDebeziumConnectorAsync();
        await CreateClickHouseAnalyticsSchemaAsync();
    }

    public async Task DisposeAsync()
    {
        await _kafkaConnect.DisposeAsync();
        await _clickHouse.DisposeAsync();
        await _kafka.DisposeAsync();
        await _postgres.DisposeAsync();
        await _network.DisposeAsync();
    }

    private async Task CreateLedgerEntriesTableAsync()
    {
        await using NpgsqlConnection connection = new(_postgres.GetConnectionString());
        await connection.OpenAsync();

        await using NpgsqlCommand command = connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE "LedgerEntries" (
                "Id" BIGSERIAL PRIMARY KEY,
                "TransactionId" UUID NOT NULL,
                "Amount" NUMERIC NOT NULL,
                "CreatedAt" TIMESTAMP NOT NULL
            );
            """;
        await command.ExecuteNonQueryAsync();
    }

    private async Task RegisterDebeziumConnectorAsync()
    {
        using HttpClient httpClient = new()
        {
            BaseAddress = new Uri($"http://{_kafkaConnect.Hostname}:{_kafkaConnect.GetMappedPublicPort(KafkaConnectPort)}")
        };

        object connectorConfig = new
        {
            name = "ledger-db-connector-test",
            config = new Dictionary<string, string>
            {
                ["connector.class"] = "io.debezium.connector.postgresql.PostgresConnector",
                ["tasks.max"] = "1",
                ["database.hostname"] = PostgresNetworkAlias,
                ["database.port"] = "5432",
                ["database.user"] = "postgres",
                ["database.password"] = "postgres",
                ["database.dbname"] = "ledger_db",
                ["topic.prefix"] = "cdc.ledger",
                ["plugin.name"] = "pgoutput",
                ["slot.name"] = "ledger_slot",
                ["publication.autocreate.mode"] = "filtered",
                ["table.include.list"] = "public.LedgerEntries",
                ["decimal.handling.mode"] = "string",
                ["time.precision.mode"] = "connect",
                ["key.converter"] = "org.apache.kafka.connect.json.JsonConverter",
                ["key.converter.schemas.enable"] = "false",
                ["value.converter"] = "org.apache.kafka.connect.json.JsonConverter",
                ["value.converter.schemas.enable"] = "false",
                ["transforms"] = "unwrap",
                ["transforms.unwrap.type"] = "io.debezium.transforms.ExtractNewRecordState",
                ["transforms.unwrap.drop.tombstones"] = "false",
                ["transforms.unwrap.delete.handling.mode"] = "rewrite"
            }
        };

        HttpResponseMessage response = await httpClient.PostAsJsonAsync("/connectors", connectorConfig);
        response.EnsureSuccessStatusCode();
    }

    private async Task CreateClickHouseAnalyticsSchemaAsync()
    {
        await using ClickHouseConnection connection = new(_clickHouse.GetConnectionString());
        await connection.OpenAsync();

        await ExecuteAsync(connection, "CREATE DATABASE IF NOT EXISTS analytics");

        await ExecuteAsync(connection, $"""
            CREATE TABLE analytics.ledger_entries_queue
            (
                Id Int64,
                TransactionId UUID,
                Amount String,
                CreatedAt String,
                __deleted String
            )
            ENGINE = Kafka
            SETTINGS
                kafka_broker_list = '{KafkaNetworkAlias}:9094',
                kafka_topic_list = 'cdc.ledger.public.LedgerEntries',
                kafka_group_name = 'clickhouse-ledger-entries-test',
                kafka_format = 'JSONEachRow',
                kafka_skip_broken_messages = 5
            """);

        await ExecuteAsync(connection, """
            CREATE TABLE analytics.ledger_entries
            (
                Id Int64,
                TransactionId UUID,
                Amount Decimal64(4),
                CreatedAt DateTime64(6)
            )
            ENGINE = MergeTree
            ORDER BY (TransactionId, Id)
            """);

        await ExecuteAsync(connection, """
            CREATE MATERIALIZED VIEW analytics.ledger_entries_mv
            TO analytics.ledger_entries
            AS
            SELECT
                Id,
                TransactionId,
                toDecimal64(Amount, 4) AS Amount,
                parseDateTime64BestEffort(CreatedAt, 6) AS CreatedAt
            FROM analytics.ledger_entries_queue
            WHERE __deleted = 'false'
            """);
    }

    private async Task<string> CollectDiagnosticsAsync()
    {
        System.Text.StringBuilder diagnostics = new();

        try
        {
            using HttpClient httpClient = new()
            {
                BaseAddress = new Uri($"http://{_kafkaConnect.Hostname}:{_kafkaConnect.GetMappedPublicPort(KafkaConnectPort)}")
            };
            string connectorStatus = await httpClient.GetStringAsync("/connectors/ledger-db-connector-test/status");
            diagnostics.AppendLine($"Connector status: {connectorStatus}");
        }
        catch (Exception ex)
        {
            diagnostics.AppendLine($"Failed to fetch connector status: {ex.Message}");
        }

        try
        {
            await using ClickHouseConnection connection = new(_clickHouse.GetConnectionString());
            await connection.OpenAsync();

            await using ClickHouseCommand command = connection.CreateCommand();
            command.CommandText = "SELECT database, table, exceptions.text[1] FROM system.kafka_consumers WHERE database = 'analytics'";
            await using System.Data.Common.DbDataReader reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                diagnostics.AppendLine($"kafka_consumers[{reader.GetString(0)}.{reader.GetString(1)}] lastError={(reader.IsDBNull(2) ? "<none>" : reader.GetString(2))}");
            }
        }
        catch (Exception ex)
        {
            diagnostics.AppendLine($"Failed to query system.kafka_consumers: {ex.Message}");
        }

        try
        {
            await using ClickHouseConnection connection = new(_clickHouse.GetConnectionString());
            await connection.OpenAsync();

            await using ClickHouseCommand command = connection.CreateCommand();
            command.CommandText = "SELECT count() FROM analytics.ledger_entries";
            object? result = await command.ExecuteScalarAsync();
            diagnostics.AppendLine($"analytics.ledger_entries row count: {result}");
        }
        catch (Exception ex)
        {
            diagnostics.AppendLine($"Failed to count analytics.ledger_entries: {ex.Message}");
        }

        return diagnostics.ToString();
    }

    private static async Task ExecuteAsync(ClickHouseConnection connection, string sql)
    {
        await using ClickHouseCommand command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task InsertingPostgresRow_ShouldStreamThroughDebeziumIntoClickHouseWithIdenticalValues()
    {
        Guid transactionId = Guid.NewGuid();
        const decimal expectedAmount = 1234.5678m;
        DateTime createdAt = new(2026, 3, 14, 8, 30, 0, DateTimeKind.Utc);

        await using (NpgsqlConnection connection = new(_postgres.GetConnectionString()))
        {
            await connection.OpenAsync();
            await using NpgsqlCommand command = connection.CreateCommand();
            command.CommandText = """INSERT INTO "LedgerEntries" ("TransactionId", "Amount", "CreatedAt") VALUES (@transactionId, @amount, @createdAt)""";
            command.Parameters.AddWithValue("transactionId", transactionId);
            command.Parameters.AddWithValue("amount", expectedAmount);
            command.Parameters.AddWithValue("createdAt", createdAt);
            await command.ExecuteNonQueryAsync();
        }

        AsyncRetryPolicy retryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(15, _ => TimeSpan.FromSeconds(2));

        (decimal Amount, DateTime CreatedAt)? replicatedRow = null;

        try
        {
            await retryPolicy.ExecuteAsync(async () =>
            {
                await using ClickHouseConnection connection = new(_clickHouse.GetConnectionString());
                await connection.OpenAsync();

                await using ClickHouseCommand command = connection.CreateCommand();
                command.CommandText = $"SELECT Amount, CreatedAt FROM analytics.ledger_entries WHERE TransactionId = '{transactionId}'";

                await using System.Data.Common.DbDataReader reader = await command.ExecuteReaderAsync();

                if (!await reader.ReadAsync())
                {
                    throw new InvalidOperationException("Row has not replicated to ClickHouse yet.");
                }

                replicatedRow = (reader.GetDecimal(0), reader.GetDateTime(1));
            });
        }
        catch (Exception ex)
        {
            string diagnostics = await CollectDiagnosticsAsync();
            throw new InvalidOperationException($"{ex.Message}\n--- Diagnostics ---\n{diagnostics}", ex);
        }

        replicatedRow.Should().NotBeNull("because the retry policy only completes once a row was actually read");
        replicatedRow!.Value.Amount.Should().Be(expectedAmount, "because the CDC pipeline must not lose or round the financial amount in transit");
        replicatedRow.Value.CreatedAt.Should().Be(createdAt, "because the replicated row must be identical to the source Postgres row, not just present");
    }
}
