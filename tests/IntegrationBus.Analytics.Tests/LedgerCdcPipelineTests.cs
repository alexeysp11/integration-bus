using System.Net.Http.Json;
using ClickHouse.Client.ADO;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Images;
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
/// Postgres source connector, streamed onto a Kafka CDC topic, and delivered into a ClickHouse ReplacingMergeTree
/// table by the official ClickHouse Kafka Connect Sink connector -- with zero application code involved,
/// mirroring exactly the production wiring in <c>docker-compose.yml</c>, <c>infrastructure/clickhouse/init.sql</c>
/// and <c>infrastructure/clickhouse-sink/*.json</c>. See <c>docs/data-loading.ru.md</c> for why the sink connector
/// (batched, acknowledged-before-offset-commit writes) was chosen over ClickHouse's native row-by-row Kafka engine.
/// </summary>
public sealed class LedgerCdcPipelineTests : IAsyncLifetime
{
    private const string KafkaNetworkAlias = "kafka";
    private const string PostgresNetworkAlias = "postgres";
    private const string ClickHouseNetworkAlias = "clickhouse";
    private const string ClickHousePassword = "clickhouse_dev_password";
    private const int KafkaConnectPort = 8083;

    private INetwork _network = null!;
    private PostgreSqlContainer _postgres = null!;
    private IContainer _kafka = null!;
    private IFutureDockerImage _kafkaConnectImage = null!;
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
            .WithNetworkAliases(ClickHouseNetworkAlias)
            .WithUsername("default")
            .WithPassword(ClickHousePassword)
            .WithEnvironment("CLICKHOUSE_PASSWORD", ClickHousePassword)
            .Build();

        // Builds the exact same custom image docker-compose.yml builds for integration-bus-kafka-connect: the
        // official Debezium Connect image plus the official ClickHouse Kafka Connect Sink plugin.
        _kafkaConnectImage = new ImageFromDockerfileBuilder()
            .WithDockerfileDirectory(CommonDirectoryPath.GetGitDirectory(), "infrastructure/kafka-connect")
            .WithDockerfile("Dockerfile")
            .WithDeleteIfExists(false)
            .Build();

        await _kafkaConnectImage.CreateAsync();

        await Task.WhenAll(
            _postgres.StartAsync(),
            _kafka.StartAsync(),
            _clickHouse.StartAsync());

        _kafkaConnect = new ContainerBuilder(_kafkaConnectImage)
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
        await RegisterDebeziumSourceConnectorAsync();
        await CreateClickHouseDestinationTableAsync();
        await RegisterClickHouseSinkConnectorAsync();
    }

    public async Task DisposeAsync()
    {
        await _kafkaConnect.DisposeAsync();
        await _clickHouse.DisposeAsync();
        await _kafka.DisposeAsync();
        await _postgres.DisposeAsync();
        await _network.DisposeAsync();
        await _kafkaConnectImage.DisposeAsync();
    }

    private async Task CreateLedgerEntriesTableAsync()
    {
        await using NpgsqlConnection connection = new(_postgres.GetConnectionString());
        await connection.OpenAsync();

        await using NpgsqlCommand command = connection.CreateCommand();
        // Matches the real EF Core migration exactly (src/Services/IntegrationBus.CoreLedger.Service/Migrations):
        // "timestamp with time zone", not a plain "timestamp" -- Debezium serializes the former as an ISO-8601
        // string (ZonedTimestamp semantic type) and the latter as raw epoch-millis (Connect's Timestamp logical
        // type), which matters because the sink connector's date_time_input_format=best_effort only applies to
        // the string form.
        command.CommandText = """
            CREATE TABLE "LedgerEntries" (
                "Id" BIGSERIAL PRIMARY KEY,
                "TransactionId" UUID NOT NULL,
                "Amount" NUMERIC NOT NULL,
                "CreatedAt" TIMESTAMP WITH TIME ZONE NOT NULL
            );
            """;
        await command.ExecuteNonQueryAsync();
    }

    private HttpClient CreateKafkaConnectClient() => new()
    {
        BaseAddress = new Uri($"http://{_kafkaConnect.Hostname}:{_kafkaConnect.GetMappedPublicPort(KafkaConnectPort)}")
    };

    private async Task RegisterDebeziumSourceConnectorAsync()
    {
        using HttpClient httpClient = CreateKafkaConnectClient();

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

    private async Task RegisterClickHouseSinkConnectorAsync()
    {
        using HttpClient httpClient = CreateKafkaConnectClient();

        object connectorConfig = new
        {
            name = "clickhouse-sink-ledger-test",
            config = new Dictionary<string, string>
            {
                ["connector.class"] = "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
                ["tasks.max"] = "1",
                ["topics"] = "cdc.ledger.public.LedgerEntries",
                ["topic2TableMap"] = "cdc.ledger.public.LedgerEntries=ledger_entries",
                ["hostname"] = ClickHouseNetworkAlias,
                ["port"] = "8123",
                ["database"] = "analytics",
                ["username"] = "default",
                ["password"] = ClickHousePassword,
                ["ssl"] = "false",
                ["exactlyOnce"] = "false",
                // The plugin's default V1 client fails to ping this ClickHouse version; V2 is the modern,
                // actively maintained client and is what production uses (see infrastructure/clickhouse-sink/*.json).
                ["client_version"] = "V2",
                // ClickHouse's strict (non-best-effort) DateTime64 parser rejects Debezium's ISO-8601 timestamps
                // (e.g. "2026-03-14T08:30:00.000000Z") without this setting.
                ["clickhouseSettings"] = "date_time_input_format=best_effort",
                ["key.converter"] = "org.apache.kafka.connect.json.JsonConverter",
                ["key.converter.schemas.enable"] = "false",
                ["value.converter"] = "org.apache.kafka.connect.json.JsonConverter",
                ["value.converter.schemas.enable"] = "false",
                ["errors.tolerance"] = "none",
                ["errors.log.enable"] = "true"
            }
        };

        HttpResponseMessage response = await httpClient.PostAsJsonAsync("/connectors", connectorConfig);
        response.EnsureSuccessStatusCode();
    }

    private async Task CreateClickHouseDestinationTableAsync()
    {
        await using ClickHouseConnection connection = new(_clickHouse.GetConnectionString());
        await connection.OpenAsync();

        await ExecuteAsync(connection, "CREATE DATABASE IF NOT EXISTS analytics");

        await ExecuteAsync(connection, """
            CREATE TABLE analytics.ledger_entries
            (
                Id Int64,
                TransactionId UUID,
                Amount Decimal64(4),
                CreatedAt DateTime64(6),
                __deleted String DEFAULT 'false',
                IngestedAtUtc DateTime DEFAULT now()
            )
            ENGINE = ReplacingMergeTree
            ORDER BY (TransactionId, Id)
            SETTINGS non_replicated_deduplication_window = 100
            """);
    }

    private async Task<string> CollectDiagnosticsAsync()
    {
        System.Text.StringBuilder diagnostics = new();

        using HttpClient httpClient = CreateKafkaConnectClient();
        foreach (string connectorName in new[] { "ledger-db-connector-test", "clickhouse-sink-ledger-test" })
        {
            try
            {
                string connectorStatus = await httpClient.GetStringAsync($"/connectors/{connectorName}/status");
                diagnostics.AppendLine($"Connector '{connectorName}' status: {connectorStatus}");
            }
            catch (Exception ex)
            {
                diagnostics.AppendLine($"Failed to fetch status for '{connectorName}': {ex.Message}");
            }
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
    public async Task InsertingPostgresRow_ShouldStreamThroughDebeziumAndTheClickHouseSinkConnectorWithIdenticalValues()
    {
        Guid transactionId = Guid.NewGuid();
        const decimal expectedAmount = 1234.5678m;
        DateTime createdAt = new DateTime(2026, 3, 14, 8, 30, 0, DateTimeKind.Utc).AddTicks(1234560);

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
                // CreatedAt is read back as a string: ClickHouse.Client's ADO DateTime64(6) reader mis-scales
                // sub-millisecond precision, which is a client-library quirk, not a replication bug -- parsing
                // ClickHouse's own canonical text representation sidesteps it entirely.
                command.CommandText = $"SELECT Amount, toString(CreatedAt) FROM analytics.ledger_entries WHERE TransactionId = '{transactionId}'";

                await using System.Data.Common.DbDataReader reader = await command.ExecuteReaderAsync();

                if (!await reader.ReadAsync())
                {
                    throw new InvalidOperationException("Row has not replicated to ClickHouse yet.");
                }

                DateTime createdAtUtc = DateTime.SpecifyKind(
                    DateTime.Parse(reader.GetString(1), System.Globalization.CultureInfo.InvariantCulture),
                    DateTimeKind.Utc);
                replicatedRow = (reader.GetDecimal(0), createdAtUtc);
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
