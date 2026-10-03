#!/bin/sh
set -e

KAFKA_CONNECT_URL="http://integration-bus-kafka-connect:8083"

echo "Waiting for Kafka Connect REST API to become available..."
until curl -s -f -o /dev/null "$KAFKA_CONNECT_URL/connectors"; do
  sleep 2
done

# The worker can start answering this GET before it has fully joined the Connect cluster and is ready to reliably
# process POSTs -- give it a short grace period, and retry each registration call individually rather than letting
# one transient "empty reply" kill the whole provisioning run.
sleep 5

register_connector() {
  config_file="$1"
  attempt=1
  max_attempts=10

  while [ "$attempt" -le "$max_attempts" ]; do
    status_code=$(curl -s -o /tmp/response.json -w "%{http_code}" \
      -X POST "$KAFKA_CONNECT_URL/connectors" \
      -H "Content-Type: application/json" \
      --data @"$config_file" || echo "000")

    if [ "$status_code" = "201" ] || [ "$status_code" = "409" ]; then
      echo "Connector from $config_file registered (or already exists), HTTP $status_code."
      return 0
    fi

    echo "Attempt $attempt/$max_attempts: registering $config_file returned HTTP $status_code, retrying in 3s..."
    attempt=$((attempt + 1))
    sleep 3
  done

  echo "Failed to register connector from $config_file after $max_attempts attempts, last HTTP $status_code:"
  cat /tmp/response.json 2>/dev/null
  return 1
}

register_all() {
  description="$1"
  directory="$2"

  echo "Registering $description..."

  for config_file in "$directory"/*.json; do
    [ -e "$config_file" ] || continue
    echo "Registering connector from $config_file..."
    register_connector "$config_file"
  done
}

# Source connectors first: they start the CDC snapshot and lazily create the Kafka topics the sink depends on.
register_all "Debezium Postgres source connectors" /connectors/source
register_all "ClickHouse sink connectors" /connectors/sink

echo "All Kafka Connect connectors registered successfully."
