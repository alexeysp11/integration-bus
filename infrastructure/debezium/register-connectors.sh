#!/bin/sh
set -e

KAFKA_CONNECT_URL="http://integration-bus-kafka-connect:8083"

echo "Waiting for Kafka Connect REST API to become available..."
until curl -s -f -o /dev/null "$KAFKA_CONNECT_URL/connectors"; do
  sleep 2
done

echo "Kafka Connect is ready. Registering Debezium Postgres source connectors..."

for config_file in /connectors/*.json; do
  connector_name=$(basename "$config_file" .json)
  echo "Registering connector from $config_file..."

  status_code=$(curl -s -o /tmp/response.json -w "%{http_code}" \
    -X POST "$KAFKA_CONNECT_URL/connectors" \
    -H "Content-Type: application/json" \
    --data @"$config_file")

  if [ "$status_code" = "201" ] || [ "$status_code" = "409" ]; then
    echo "Connector from $config_file registered (or already exists), HTTP $status_code."
  else
    echo "Failed to register connector from $config_file, HTTP $status_code:"
    cat /tmp/response.json
    exit 1
  fi
done

echo "All Debezium connectors registered successfully."
