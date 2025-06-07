#!/usr/bin/env bash
# benchmark.sh
# Usage: ./benchmark.sh
# This script benchmarks the overhead of Postgres Online Schema Tool using pgbench.
# It uses the init_db script for setup.

set -euo pipefail

PG_BENCH_ARGS="-c 4 -j 4 -T 60"

function run_pgbench() {
    local label="$1"
    echo "Running pgbench: $label"
    pgbench $PG_BENCH_ARGS -h localhost -U post_test post_test | tee "/tmp/pgbench_${label}.out"
}

function extract_tps() {
    grep "including connections establishing" "$1" | awk '{print $3}'
}

# 1. Baseline: clean DB
./scripts/init_db
run_pgbench "baseline"

# 2. Triggers + log replay (replay-only)
./scripts/init_db
# Start pgbench first, then pg-ost replay-only
run_pgbench "triggers_replay" &
PGBENCH_PID=$!
sleep 2
./target/debug/postgres-ost replay-only --uri "postgres://post_test@localhost/post_test" --sql "ALTER TABLE pgbench_accounts ADD COLUMN "purchased" BOOLEAN DEFAULT FALSE;" &
REPLAY_PID=$!
wait $PGBENCH_PID
kill -INT $REPLAY_PID
wait $REPLAY_PID 2>/dev/null || true

# Print comparison table
BASELINE_TPS=$(extract_tps /tmp/pgbench_baseline.out)
TRIGGERS_TPS=$(extract_tps /tmp/pgbench_triggers_replay.out)

printf "\nBenchmark Results (pgbench TPS)\n"
printf "%-20s %-10s\n" "Scenario" "TPS"
printf "%-20s %-10s\n" "--------" "---"
printf "%-20s %-10s\n" "Baseline" "$BASELINE_TPS"
printf "%-20s %-10s\n" "Triggers+Replay" "$TRIGGERS_TPS"

# Add more scenarios as needed.
