#!/usr/bin/env bash
# Verifies end-to-end financial consistency after a load or chaos run. Exit code 0 means every invariant held.
# Usage: reconcile.sh <work-dir> [run-started-at-utc]
#   <work-dir>/k6-summary.json (optional) provides the number of transactions the API acknowledged with HTTP 202.
# Invariants and rationale: docs/chaos-engineering/README.md (relative to the repository root).
set -uo pipefail
source "$(dirname "$0")/lib.sh"

WORK_DIR="${1:?work directory required}"
RUN_STARTED_AT="${2:-1970-01-01 00:00:00+00}"
DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-240}"
FAILURES=0

check() { # check <name> <expected> <actual>
    if [ "$2" = "$3" ]; then
        printf 'PASS  %-62s expected=%s actual=%s\n' "$1" "$2" "$3"
    else
        printf 'FAIL  %-62s expected=%s actual=%s\n' "$1" "$2" "$3"
        FAILURES=$((FAILURES + 1))
    fi
}

NON_TERMINAL_SQL="SELECT COUNT(*) FROM \"TransactionState\" WHERE \"CreatedAt\" >= '$RUN_STARTED_AT' AND \"CurrentState\" NOT IN ('Completed', 'Failed')"

log "waiting up to ${DRAIN_TIMEOUT}s for in-flight sagas to reach a terminal state"
DEADLINE=$((SECONDS + DRAIN_TIMEOUT))
until [ "$(db saga_db "$NON_TERMINAL_SQL")" = "0" ] || [ "$SECONDS" -ge "$DEADLINE" ]; do sleep 3; done

SAGA_TOTAL="$(db saga_db "SELECT COUNT(*) FROM \"TransactionState\" WHERE \"CreatedAt\" >= '$RUN_STARTED_AT'")"
SAGA_COMPLETED="$(db saga_db "SELECT COUNT(*) FROM \"TransactionState\" WHERE \"CreatedAt\" >= '$RUN_STARTED_AT' AND \"CurrentState\" = 'Completed'")"
SAGA_FAILED="$(db saga_db "SELECT COUNT(*) FROM \"TransactionState\" WHERE \"CreatedAt\" >= '$RUN_STARTED_AT' AND \"CurrentState\" = 'Failed'")"
log "sagas: total=$SAGA_TOTAL completed=$SAGA_COMPLETED failed=$SAGA_FAILED"

# No accepted transaction may be dropped: every HTTP 202 must have produced exactly one saga instance
if [ -f "$WORK_DIR/k6-summary.json" ]; then
    ACCEPTED="$(grep -o '"acceptedTransactions": *[0-9]*' "$WORK_DIR/k6-summary.json" | grep -o '[0-9]*$')"
    check "every accepted transaction produced exactly one saga" "$ACCEPTED" "$SAGA_TOTAL"
fi

check "no saga is stuck in a non-terminal state" "0" "$(db saga_db "$NON_TERMINAL_SQL")"

# Money conservation: holds, releases and confirmations only move value, so the journal total equals the deposits
JOURNAL_TOTAL="$(db accounting_db 'SELECT COALESCE(SUM("AmountDelta"), 0) FROM "JournalEntries"')"
DEPOSIT_TOTAL="$(db accounting_db 'SELECT COALESCE(SUM("AmountDelta"), 0) FROM "JournalEntries" WHERE "EntryType" = 4')"
check "money is neither created nor destroyed (journal sum == deposits)" "$DEPOSIT_TOTAL" "$JOURNAL_TOTAL"

check "no account has a negative reconstructed balance" "0" "$(db accounting_db 'SELECT COUNT(*) FROM (SELECT "SourceAccountId" FROM "JournalEntries" GROUP BY "SourceAccountId" HAVING SUM("AmountDelta") < 0) negative')"
check "no duplicate journal sequence numbers" "0" "$(db accounting_db 'SELECT COUNT(*) FROM (SELECT 1 FROM "JournalEntries" GROUP BY "SourceAccountId", "SequenceNumber" HAVING COUNT(*) > 1) duplicates')"

# Cross-database comparison of the saga outcome against the journal and the ledger of every transaction
db saga_db "SELECT \"CorrelationId\", \"CurrentState\" FROM \"TransactionState\" WHERE \"CreatedAt\" >= '$RUN_STARTED_AT' ORDER BY 1" > "$WORK_DIR/saga.txt"
db accounting_db 'SELECT "TransactionId", COUNT(*) FILTER (WHERE "EntryType" = 1), COUNT(*) FILTER (WHERE "EntryType" = 2), COUNT(*) FILTER (WHERE "EntryType" = 3) FROM "JournalEntries" WHERE "EntryType" IN (1, 2, 3) GROUP BY 1 ORDER BY 1' > "$WORK_DIR/journal.txt"
db ledger_db 'SELECT "TransactionId", COUNT(*) FROM "LedgerEntries" GROUP BY 1 ORDER BY 1' > "$WORK_DIR/ledger.txt"

VIOLATIONS="$(awk -F'|' '
    FILENAME == ARGV[1] { state[$1] = $2; next }
    FILENAME == ARGV[2] { holds[$1] = $2; confirms[$1] = $3; cancels[$1] = $4; next }
    FILENAME == ARGV[3] { ledger[$1] = $2; next }
    END {
        for (id in state) {
            h = holds[id] + 0; c = confirms[id] + 0; x = cancels[id] + 0; l = ledger[id] + 0
            if (state[id] == "Completed" && !(h == 1 && c == 2 && x == 0 && l == 1)) bad["completed"]++
            if (state[id] == "Failed" && !(c == 0 && x == h && l == 0)) bad["failed"]++
        }
        for (id in holds) if (!(id in state) && (holds[id] + 0) > 0) bad["orphan-journal"]++
        for (id in ledger) if (!(id in state)) bad["orphan-ledger"]++
        for (k in bad) printf "%s=%d ", k, bad[k]
    }' "$WORK_DIR/saga.txt" "$WORK_DIR/journal.txt" "$WORK_DIR/ledger.txt")"

check "Completed sagas have 1 hold, 2 confirmations, 0 releases, 1 ledger entry" "" "$(echo "$VIOLATIONS" | grep -o 'completed=[0-9]*')"
check "Failed sagas left no confirmation, no ledger entry and a fully released hold" "" "$(echo "$VIOLATIONS" | grep -o 'failed=[0-9]*')"
check "no journal or ledger records exist without a saga" "" "$(echo "$VIOLATIONS" | grep -oE 'orphan-(journal|ledger)=[0-9]*' | tr '\n' ' ')"

if [ "$FAILURES" -eq 0 ]; then
    log "RESULT: all invariants held"
else
    log "RESULT: $FAILURES invariant(s) violated"
fi
exit "$FAILURES"
