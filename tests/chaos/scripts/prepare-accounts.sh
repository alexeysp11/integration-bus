#!/usr/bin/env bash
# Seeds a pool of USD accounts, funds each of them and writes the account ids to accounts.json for k6.
# Usage: prepare-accounts.sh <work-dir> [account-count] [initial-balance]
set -euo pipefail
source "$(dirname "$0")/lib.sh"

WORK_DIR="${1:?work directory required}"
COUNT="${2:-300}"
INITIAL_BALANCE="${3:-1000}"
mkdir -p "$WORK_DIR"

BEFORE="$(db accounting_db 'SELECT COUNT(*) FROM "Accounts"')"
log "seeding $COUNT accounts (existing: $BEFORE)"
curl -fsS -X POST "$PROCESSING_URL/api/v1/accounts/seed" -H 'Content-Type: application/json' \
    -d "{\"count\": $COUNT, \"currency\": 1}" >/dev/null

for _ in $(seq 1 60); do
    CURRENT="$(db accounting_db 'SELECT COUNT(*) FROM "Accounts"')"
    [ "$((CURRENT - BEFORE))" -ge "$COUNT" ] && break
    sleep 1
done

db accounting_db "SELECT \"Id\" FROM \"Accounts\" ORDER BY \"CreatedAt\" DESC, \"Id\" LIMIT $COUNT" \
    | awk 'BEGIN{printf "["} {printf "%s\"%s\"", (NR>1?",":""), $1} END{print "]"}' > "$WORK_DIR/accounts.json"

log "funding accounts with $INITIAL_BALANCE each"
tr -d '[]"' < "$WORK_DIR/accounts.json" | tr ',' '\n' | xargs -P 16 -I{} \
    curl -fsS -o /dev/null -X POST "$PROCESSING_URL/api/v1/accounts/{}/topup" \
    -H 'Content-Type: application/json' -d "{\"amount\": $INITIAL_BALANCE, \"currency\": 1}"

for _ in $(seq 1 120); do
    FUNDED="$(db accounting_db 'SELECT COUNT(DISTINCT "SourceAccountId") FROM "JournalEntries" WHERE "EntryType" = 4')"
    [ "$FUNDED" -ge "$COUNT" ] && break
    sleep 1
done
log "prepared $COUNT funded accounts in $WORK_DIR/accounts.json"
