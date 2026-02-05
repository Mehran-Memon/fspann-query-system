#!/usr/bin/env bash
# ============================================================
# FSP-ANN 100M "BALANCED SHARDED" RUN
# Strategy: ZGC + 160GB Heap (Leaves ~200GB for RocksDB/OS Cache)
# Architecture: 16 Shards to break the 78M lock contention limit
# ============================================================

# 1. Detect Paths
PROJECT_ROOT="$(pwd)"
JAR="$PROJECT_ROOT/api/target/api-0.0.1-SNAPSHOT-shaded.jar"
CONFIG_DIR="$PROJECT_ROOT/config/src/main/resources"
BASE_DIR="/mnt/data/mehran/Datasets/SIFT1B"
OUTPUT_ROOT="/mnt/data/mehran/SCALABILITY_RUNS"
LARGE_TMP="/mnt/data/mehran/tmp"

# 2. Input Files (SIFT 100M Subset)
# Ensure config_sift1b_10m.json is generic enough or create a 100m variant
CONFIG_FILE="$CONFIG_DIR/config_sift1b_10m.json"
DATA_FILE="$BASE_DIR/bigann_learn_100m.bvecs"
QUERY_FILE="$BASE_DIR/bigann_query_1k.bvecs"
GT_FILE="$BASE_DIR/bigann_query_groundtruth.ivecs"

# 3. JVM Configuration (ZGC + 160GB)
# CRITICAL FIX: Xms must be <= Xmx. Both set to 160g.
JVM_ARGS=(
  "-server"
  "-XX:+UseZGC"
  "-XX:+AlwaysPreTouch"
  "-Xms160g"
  "-Xmx160g"
  "-Dfile.encoding=UTF-8"
  "-Djava.io.tmpdir=$LARGE_TMP"
  "-Dreenc.mode=end"
  "-Dmetadata.sharded=true"
  "-Dmetadata.shards=16"
)

# 4. Experiment Config
DIMENSION=128
BATCH_SIZE=250000

echo "=========================================="
echo "   STARTING 100M SHARDED RUN              "
echo "   Target: 16 Shards, 160GB Heap          "
echo "   Start Time: $(date)                    "
echo "=========================================="

mkdir -p "$OUTPUT_ROOT"
mkdir -p "$LARGE_TMP"

RUN_DIR="$OUTPUT_ROOT/SIFT1B-100M-SHARDED"
LOG_FILE="$RUN_DIR/run.log"

# 5. Validation
if [[ ! -f "$DATA_FILE" ]]; then
    echo "ERROR: Data file missing: $DATA_FILE"
    echo "   Please ensure 'bigann_learn_100m.bvecs' exists."
    exit 1
fi

if [[ ! -f "$JAR" ]]; then
    echo "ERROR: JAR file missing at $JAR"
    exit 1
fi

echo "Cleaning artifacts..."
rm -rf "$RUN_DIR"
mkdir -p "$RUN_DIR/results"

# 6. Execution
echo "Executing FSP-ANN with Sharding..."
echo "JVM Args: ${JVM_ARGS[*]}"

nohup java "${JVM_ARGS[@]}" -jar "$JAR" \
  "$CONFIG_FILE" "$DATA_FILE" "$QUERY_FILE" \
  "$RUN_DIR/keys.blob" "$DIMENSION" "$RUN_DIR" \
  "$GT_FILE" "$BATCH_SIZE" > "$LOG_FILE" 2>&1 &

PID=$!
echo "Process started with PID: $PID"
echo "   Logs: $LOG_FILE"
echo "   Monitor: tail -f $LOG_FILE"