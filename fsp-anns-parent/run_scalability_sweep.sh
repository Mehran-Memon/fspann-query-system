#!/usr/bin/env bash
# ROBUST RESUME SCRIPT: Starts at 25M -> 100M
# Removed 'set -e' to prevent minor logging errors from killing the loop

# -------------------- AUTO-DETECT PATHS --------------------
PROJECT_ROOT="$(pwd)"
JAR="$PROJECT_ROOT/api/target/api-0.0.1-SNAPSHOT-shaded.jar"
CONFIG_DIR="$PROJECT_ROOT/config/src/main/resources"

# Data Paths (Absolute)
BASE_DIR="/mnt/data/mehran/Datasets/SIFT1B"
OUTPUT_ROOT="/mnt/data/mehran/SCALABILITY_RUNS"
LARGE_TMP="/mnt/data/mehran/tmp"

# Inputs
CONFIG_FILE="$CONFIG_DIR/config_sift1b_10m.json"
QUERY_FILE="$BASE_DIR/bigann_query_1k.bvecs"
GT_FILE="$BASE_DIR/bigann_query_groundtruth.ivecs"

# -------------------- JVM CONFIG --------------------
# Using large heap for 100M run stability
JVM_ARGS=(
  "-server"
  "-XX:+UseG1GC"
  "-XX:MaxGCPauseMillis=200"
  "-XX:+AlwaysPreTouch"
  "-Xms250g"
  "-Xmx300g"
  "-Dfile.encoding=UTF-8"
  "-Djava.io.tmpdir=$LARGE_TMP"
  "-Dreenc.mode=end"
)

# -------------------- EXPERIMENT CONFIG --------------------
DIMENSION=128
BATCH_SIZE=100000

# !!! 10M REMOVED - STARTING AT 25M !!!
SIZES=("25m" "50m" "75m" "100m")

echo "=========================================="
echo "   RESUMING SCALABILITY SWEEP             "
echo "   Target: ${SIZES[*]}                    "
echo "   Start Time: $(date)                    "
echo "=========================================="

mkdir -p "$OUTPUT_ROOT"
mkdir -p "$LARGE_TMP"

for SIZE in "${SIZES[@]}"; do
    echo ""
    echo "######################################################"
    echo "▶ STARTING EXPERIMENT: SIFT1B-${SIZE}"
    echo "######################################################"

    DATA_FILE="$BASE_DIR/bigann_learn_${SIZE}.bvecs"
    RUN_DIR="$OUTPUT_ROOT/SIFT1B-${SIZE}"
    LOG_FILE="$RUN_DIR/run.log"

    # Validation
    if [[ ! -f "$DATA_FILE" ]]; then
        echo "⚠️  Skipping ${SIZE}: Data file missing ($DATA_FILE)"
        continue
    fi

    # Clean previous run artifacts
    rm -rf "$RUN_DIR"
    mkdir -p "$RUN_DIR/results"

    START_TIME=$(date +%s)

    # Execute Java
    echo "  • Executing..."
    java "${JVM_ARGS[@]}" -jar "$JAR" \
      "$CONFIG_FILE" \
      "$DATA_FILE" \
      "$QUERY_FILE" \
      "$RUN_DIR/keys.blob" \
      "$DIMENSION" \
      "$RUN_DIR" \
      "$GT_FILE" \
      "$BATCH_SIZE" \
      > "$LOG_FILE" 2>&1

    # Capture Exit Code
    EXIT_CODE=$?

    if [ $EXIT_CODE -eq 0 ]; then
        END_TIME=$(date +%s)
        DURATION=$((END_TIME - START_TIME))
        MINS=$((DURATION / 60))

        # Extract Time Robustly
        INDEX_TIME=$(grep "Index complete | time=" "$LOG_FILE" | tail -n 1 | awk -F'=' '{print $2}' | tr -d 's' || echo "N/A")

        echo "✅ DONE: SIFT1B-${SIZE}"
        echo "  ----------------------------------------"
        echo "  • Duration:    ${MINS} min"
        echo "  • Index Time:  ${INDEX_TIME} sec"
        echo "  • Log:         $LOG_FILE"
        echo "  ----------------------------------------"
    else
        echo "❌ FAILED: SIFT1B-${SIZE} (Exit Code: $EXIT_CODE)"
        echo "  • Check log: $LOG_FILE"
        # Continue to next size? Let's keep going in case it's a fluke.
    fi

    # Cooldown to let disk buffers flush
    sleep 30
done

echo ""
echo "=========================================="
echo "   SWEEP COMPLETE: $(date)"
echo "=========================================="