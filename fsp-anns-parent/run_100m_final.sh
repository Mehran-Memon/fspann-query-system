#!/usr/bin/env bash
# ============================================================
# FSP-ANN 100M "NUCLEAR" ATTEMPT
# Strategy: Use ZGC to prevent GC Death Spiral
# ============================================================

# -------------------- AUTO-DETECT PATHS --------------------
PROJECT_ROOT="$(pwd)"
JAR="$PROJECT_ROOT/api/target/api-0.0.1-SNAPSHOT-shaded.jar"
CONFIG_DIR="$PROJECT_ROOT/config/src/main/resources"

BASE_DIR="/mnt/data/mehran/Datasets/SIFT1B"
OUTPUT_ROOT="/mnt/data/mehran/SCALABILITY_RUNS"
LARGE_TMP="/mnt/data/mehran/tmp"

# Reuse the 10m config structure
CONFIG_FILE="$CONFIG_DIR/config_sift1b_10m.json"
QUERY_FILE="$BASE_DIR/bigann_query_1k.bvecs"
GT_FILE="$BASE_DIR/bigann_query_groundtruth.ivecs"

# -------------------- JVM CONFIG (THE FIX) --------------------
# We switch from G1GC to ZGC.
# ZGC is designed for massive heaps (up to 16TB) without long pauses.
JVM_ARGS=(
  "-server"
  "-XX:+UseZGC"               # <--- THE KEY CHANGE
  "-Xms220g"                  # Lowered slightly to leave room for RocksDB off-heap
  "-Xmx220g"
  "-XX:+AlwaysPreTouch"       # Commit RAM immediately to prevent OS lag later
  "-Dfile.encoding=UTF-8"
  "-Djava.io.tmpdir=$LARGE_TMP"
  "-Dreenc.mode=end"
)

# -------------------- EXPERIMENT CONFIG --------------------
DIMENSION=128
BATCH_SIZE=100000

echo "=========================================="
echo "   STARTING 100M VECTOR INDEXING (ZGC)    "
echo "   Heap: 220GB (ZGC Enabled)              "
echo "   Start Time: $(date)                    "
echo "=========================================="

mkdir -p "$OUTPUT_ROOT"
mkdir -p "$LARGE_TMP"

# -------------------- 100M EXECUTION --------------------
DATA_FILE="$BASE_DIR/bigann_learn_100m.bvecs"
RUN_DIR="$OUTPUT_ROOT/SIFT1B-100M-ZGC"
LOG_FILE="$RUN_DIR/run.log"

if [[ ! -f "$DATA_FILE" ]]; then
    echo "❌ ERROR: Data file missing: $DATA_FILE"
    exit 1
fi

# Clean previous failed run
echo "Cleaning previous artifacts..."
rm -rf "$RUN_DIR"
mkdir -p "$RUN_DIR/results"

START_TIME=$(date +%s)

echo "▶ Executing 100M Ingestion..."
echo "  Log: $LOG_FILE"

# Run Java
if java "${JVM_ARGS[@]}" -jar "$JAR" \
  "$CONFIG_FILE" \
  "$DATA_FILE" \
  "$QUERY_FILE" \
  "$RUN_DIR/keys.blob" \
  "$DIMENSION" \
  "$RUN_DIR" \
  "$GT_FILE" \
  "$BATCH_SIZE" \
  > "$LOG_FILE" 2>&1; then

    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    MINS=$((DURATION / 60))
    HOURS=$((DURATION / 3600))

    # Extract Indexing Time
    INDEX_TIME=$(grep "Index complete | time=" "$LOG_FILE" | tail -n 1 | awk -F'=' '{print $2}' | tr -d 's' || echo "N/A")

    echo "✅ SUCCESS: SIFT1B-100M"
    echo "  ----------------------------------------"
    echo "  • Duration:    ${HOURS}h ${MINS}m"
    echo "  • Index Time:  ${INDEX_TIME} sec"
    echo "  ----------------------------------------"
else
    echo "❌ FAILED: SIFT1B-100M"
    echo "  • Check log: $LOG_FILE"
    exit 1
fi

echo "=========================================="
echo "   FINISHED: $(date)"
echo "=========================================="