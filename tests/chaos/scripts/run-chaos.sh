#!/usr/bin/env bash
# Runs one chaos scenario: k6 load + fault injection + post-run consistency reconciliation.
# Usage: run-chaos.sh <scenario> [duration]
#   scenarios: baseline | kill-ledger | kill-balance | kill-orchestrator | redis-latency | redis-blackhole
#   env: BACKEND=compose|k8s (default compose), TARGET_RPS (default 200), ACCOUNT_COUNT (default 300)
# Scenario catalogue and prerequisites: docs/chaos-engineering/README.md (relative to the repository root).
set -euo pipefail
source "$(dirname "$0")/lib.sh"

SCENARIO="${1:?scenario required: baseline|kill-ledger|kill-balance|kill-orchestrator|redis-latency|redis-blackhole}"
DURATION="${2:-2m}"
TARGET_RPS="${TARGET_RPS:-200}"
ACCOUNT_COUNT="${ACCOUNT_COUNT:-300}"
K6_IMAGE="${K6_IMAGE:-grafana/k6:0.54.0}"

CHAOS_DIR="$(cd "$(dirname "$0")/.." && pwd)"
WORK_DIR="${WORK_DIR:-$CHAOS_DIR/work/$SCENARIO}"
mkdir -p "$WORK_DIR"
rm -f "$WORK_DIR/k6-summary.json"

# Docker Desktop on Windows needs native paths for bind mounts; Linux and macOS use the path as is
native_path() { if command -v cygpath >/dev/null 2>&1; then cygpath -w "$1"; else echo "$1"; fi; }

K6_TARGET_HOST="host.docker.internal"
if [ "$BACKEND" = "k8s" ]; then K6_BASE_URL="http://$K6_TARGET_HOST:15201"; else K6_BASE_URL="http://$K6_TARGET_HOST:5201"; fi

inject_fault() {
    # Let the pipeline reach steady state before the fault hits
    sleep "${FAULT_DELAY_SECONDS:-30}"
    case "$SCENARIO" in
        baseline) ;;
        kill-ledger) kill_instance core-ledger-service; sleep 15; kill_instance core-ledger-service ;;
        kill-balance) kill_instance account-balance-service; sleep 15; kill_instance account-balance-service ;;
        kill-orchestrator) kill_instance saga-orchestrator-service; sleep 15; kill_instance saga-orchestrator-service ;;
        redis-latency)
            log "adding 2.5s +/- 1.5s latency to every Redis connection for 30s"
            toxiproxy POST /proxies/redis/toxics '{"name":"latency","type":"latency","attributes":{"latency":2500,"jitter":1500}}' >/dev/null
            sleep 30
            toxiproxy DELETE /proxies/redis/toxics/latency >/dev/null ;;
        redis-blackhole)
            log "black-holing Redis traffic for 20s (connections stay open, no bytes flow)"
            toxiproxy POST /proxies/redis/toxics '{"name":"blackhole","type":"timeout","attributes":{"timeout":0}}' >/dev/null
            sleep 20
            toxiproxy DELETE /proxies/redis/toxics/blackhole >/dev/null ;;
        *) log "unknown scenario $SCENARIO"; exit 2 ;;
    esac
    log "fault injection finished"
}

case "$SCENARIO" in
    redis-*)
        toxiproxy GET /proxies/redis >/dev/null 2>&1 || {
            log "Toxiproxy is not reachable at $TOXIPROXY_URL. Start the stack with the chaos profile and REDIS_ENDPOINT (compose) or chaos.toxiproxy.enabled=true (Helm)."
            exit 2
        } ;;
esac

[ -f "$WORK_DIR/accounts.json" ] || "$(dirname "$0")/prepare-accounts.sh" "$WORK_DIR" "$ACCOUNT_COUNT"

RUN_STARTED_AT="$(db saga_db "SELECT to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') || '+00'")"
log "scenario=$SCENARIO duration=$DURATION rps=$TARGET_RPS started_at=$RUN_STARTED_AT"

inject_fault &
FAULT_PID=$!

MSYS_NO_PATHCONV=1 docker run --rm --add-host host.docker.internal:host-gateway \
    -v "$(native_path "$WORK_DIR"):/work" \
    -v "$(native_path "$CHAOS_DIR/k6"):/scripts:ro" \
    -e BASE_URL="$K6_BASE_URL" -e TARGET_RPS="$TARGET_RPS" -e DURATION="$DURATION" \
    "$K6_IMAGE" run /scripts/ledger-load.js || log "k6 reported threshold failures (the reconciliation below is authoritative)"

wait "$FAULT_PID" || true

log "reconciling"
"$(dirname "$0")/reconcile.sh" "$WORK_DIR" "$RUN_STARTED_AT"
