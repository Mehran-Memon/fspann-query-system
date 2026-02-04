cat << 'EOF' > run_100m.sh
#!/usr/bin/env bash
# ============================================================
# FSP-ANN 100M "BALANCED" ATTEMPT
# Strategy: ZGC + 160GB Heap (Leaves 200GB for OS Cache)
# ============================================================

# Detect Paths
PROJECT_ROOT="$(pwd)"
JAR="$PROJECT_ROOT/api/target/api-0.0.1-SNAPSHOT-shaded.jar"
CONFIG_DIR="$PROJECT_ROOT/config/src/main/resources"
BASE_DIR="/mnt/data/mehran/Datasets/SIFT1B"
OUTPUT_ROOT="/mnt/data/mehran/SCALABILITY_RUNS"
LARGE_TMP="/mnt/data/mehran/tmp"

# Inputs
CONFIG_FILE="$CONFIG_DIR/config_sift1b_10m.json"
QUERY_FILE="$BASE_DIR/bigann_query_1k.bvecs"
GT_FILE="$BASE_DIR/bigann_query_groundtruth.ivecs"

# JVM Configuration (ZGC + 160GB)
JVM_ARGS=(
  "-server"
  "-XX:+UseZGC"
  "-XX:+AlwaysPreTouch"
  "-Xms160g"
  "-Xmx160g"
  "-Dfile.encoding=UTF-8"
  "-Djava.io.tmpdir=$LARGE_TMP"
  "-Dreenc.mode=end"
)

# Experiment Config
DIMENSION=128
BATCH_SIZE=250000

echo "=========================================="
echo "   STARTING 100M RUN (BALANCED)           "
echo "   Start Time: $(date)                    "
echo "=========================================="

mkdir -p "$OUTPUT_ROOT"
mkdir -p "$LARGE_TMP"

DATA_FILE="$BASE_DIR/bigann_learn_100m.bvecs"
RUN_DIR="$OUTPUT_ROOT/SIFT1B-100M-BALANCED"
LOG_FILE="$RUN_DIR/run.log"

if [[ ! -f "$DATA_FILE" ]]; then
    echo "❌ ERROR: Data file missing: $DATA_FILE"
    exit 1
fi

echo "Cleaning artifacts..."
rm -rf "$RUN_DIR"
mkdir -p "$RUN_DIR/results"

echo "▶ Executing..."
java "${JVM_ARGS[@]}" -jar "$JAR" \
  "$CONFIG_FILE" "$DATA_FILE" "$QUERY_FILE" \
  "$RUN_DIR/keys.blob" "$DIMENSION" "$RUN_DIR" \
  "$GT_FILE" "$BATCH_SIZE" > "$LOG_FILE" 2>&1 &

PID=$!
echo "✅ Process started with PID: $PID"
echo "   Monitor with: tail -f $LOG_FILE"
EOF