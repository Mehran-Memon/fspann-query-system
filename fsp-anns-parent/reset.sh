#!/usr/bin/env bash
# ============================================================
# FSP-ANN / JVM / RocksDB SYSTEM RESET SCRIPT (Ubuntu)
# ============================================================
# - Kills stalled JVM + test runners
# - Cleans RocksDB locks + temp dirs
# - Drops Linux page cache
# - Resets swap
# - Clears JVM temp artifacts
# ============================================================

set -euo pipefail

echo "=============================================="
echo " FSP-ANN SYSTEM RESET START"
echo "=============================================="

# --- 1. Kill stalled JVM / Maven / ANN processes ---
echo "[1/8] Killing JVM / Maven / ANN processes..."

pkill -9 -f java       || true
pkill -9 -f surefire   || true
pkill -9 -f failsafe   || true
pkill -9 -f maven      || true
pkill -9 -f mvn        || true
pkill -9 -f fspann     || true
pkill -9 -f rocksdb    || true

sleep 2

# --- 2. Kill any remaining Java PIDs ---
echo "[2/8] Forcing kill of remaining Java processes..."

for pid in $(ps aux | grep java | grep -v grep | awk '{print $2}'); do
    kill -9 "$pid" || true
done

# --- 3. Remove RocksDB LOCK files ---
echo "[3/8] Removing RocksDB LOCK files..."

find /tmp -type f -name "LOCK" -delete || true

# --- 4. Clean temp directories ---
echo "[4/8] Cleaning temp directories..."

rm -rf /tmp/junit-*            || true
rm -rf /tmp/rocksdb*           || true
rm -rf /tmp/fspann*            || true
rm -rf /tmp/hsperfdata_*       || true
rm -rf ~/.java                 || true

# --- 5. Drop Linux page cache (requires sudo) ---
echo "[5/8] Dropping Linux page cache..."

sync
echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null

# --- 6. Reset swap (requires sudo) ---
echo "[6/8] Resetting swap..."

sudo swapoff -a
sudo swapon -a

# --- 7. Kill zombie processes (best effort) ---
echo "[7/8] Cleaning zombie processes..."

for zpid in $(ps aux | awk '{ if ($8=="Z") print $2 }'); do
    ppid=$(ps -o ppid= -p "$zpid" | tr -d ' ')
    [ -n "$ppid" ] && kill -9 "$ppid" || true
done

# --- 8. Report system state ---
echo "[8/8] System state after cleanup:"
echo "----------------------------------------------"
free -h
echo
uptime
echo
ps aux | wc -l

echo "=============================================="
echo " SYSTEM RESET COMPLETE"
echo "=============================================="
