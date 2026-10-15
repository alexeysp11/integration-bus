#!/usr/bin/env bash
# Shared helpers for the chaos and load-testing scripts. Source this file; do not execute it.
# Usage and design: docs/chaos-engineering/README.md (relative to the repository root).

# BACKEND selects where the platform runs: "compose" (docker compose) or "k8s" (Helm release on Kind)
BACKEND="${BACKEND:-compose}"
K8S_NAMESPACE="${K8S_NAMESPACE:-integration-bus}"

if [ "$BACKEND" = "k8s" ]; then
    # Kind NodePort mappings, see deploy/k8s/kind/kind-cluster.yaml (compose port + 10000)
    PROCESSING_URL="${PROCESSING_URL:-http://localhost:15201}"
    TOXIPROXY_URL="${TOXIPROXY_URL:-http://localhost:18474}"
else
    PROCESSING_URL="${PROCESSING_URL:-http://localhost:5201}"
    TOXIPROXY_URL="${TOXIPROXY_URL:-http://localhost:8474}"
fi

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }

# db <database> <sql>: runs a query against one of the service databases and prints unaligned, tuples-only output
db() {
    if [ "$BACKEND" = "k8s" ]; then
        kubectl -n "$K8S_NAMESPACE" exec integration-bus-db-0 -- psql -U postgres -d "$1" -At -c "$2"
    else
        docker exec integration-bus-db psql -U postgres -d "$1" -At -c "$2"
    fi
}

# workload_name <service>: logical service name -> compose service / k8s deployment name
workload_name() { echo "integration-bus-$1"; }

# list_instances <service>: prints one instance id per line (container name or pod name)
list_instances() {
    local service
    service="$(workload_name "$1")"
    if [ "$BACKEND" = "k8s" ]; then
        kubectl -n "$K8S_NAMESPACE" get pods -l "app=$service" -o name --field-selector=status.phase=Running | sed 's|pod/||'
    else
        docker ps --filter "label=com.docker.compose.service=$service" --format '{{.Names}}'
    fi
}

# kill_instance <service>: abruptly terminates one running instance without any graceful shutdown
kill_instance() {
    local victim
    victim="$(list_instances "$1" | head -n 1)"
    [ -n "$victim" ] || { log "no running instance of $1 to kill"; return 1; }
    log "killing $victim"
    if [ "$BACKEND" = "k8s" ]; then
        kubectl -n "$K8S_NAMESPACE" delete pod "$victim" --grace-period=0 --force >&2
    else
        docker kill "$victim" >/dev/null
        # compose restarts it through "restart: unless-stopped"; start explicitly if the policy did not
        docker start "$victim" >/dev/null 2>&1 || true
    fi
}

# toxiproxy <method> <path> [json-body]: calls the Toxiproxy HTTP API
toxiproxy() {
    curl -fsS -X "$1" "$TOXIPROXY_URL$2" ${3:+-H 'Content-Type: application/json' -d "$3"}
}
