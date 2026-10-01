# Extends the official Debezium Kafka Connect worker (already carrying the Postgres source connector) with the
# official ClickHouse Kafka Connect Sink plugin, so a single Connect cluster runs both the CDC source connectors
# and the ClickHouse sink connector -- no separate Confluent-based worker required.
FROM debezium/connect:3.0.0.Final

ARG CLICKHOUSE_KAFKA_CONNECT_VERSION=1.7.0

USER root

RUN mkdir -p /kafka/connect/clickhouse-kafka-connect && \
    curl -sSL -o /tmp/clickhouse-kafka-connect.zip \
        "https://github.com/ClickHouse/clickhouse-kafka-connect/releases/download/v${CLICKHOUSE_KAFKA_CONNECT_VERSION}/clickhouse-kafka-connect-v${CLICKHOUSE_KAFKA_CONNECT_VERSION}.zip" && \
    unzip -q -j /tmp/clickhouse-kafka-connect.zip -d /kafka/connect/clickhouse-kafka-connect && \
    rm /tmp/clickhouse-kafka-connect.zip && \
    chown -R kafka:kafka /kafka/connect/clickhouse-kafka-connect

USER kafka
