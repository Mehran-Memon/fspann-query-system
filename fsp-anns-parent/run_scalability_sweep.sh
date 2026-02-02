#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

# ============================================================
# FSP-ANN SCALABILITY SWEEP (10M -> 100M)
# ============================================================
# Purpose: Prove linear indexing time and stable memory usage.
# Metrics: Index Time (s), Disk Usage (GB).
# Note: Recall metrics will be invalid due to 1B GT mismatch.
# ============================================================

echo "=========================================="
echo "   FSP-ANN BILLION-SCALE VALIDATION       "
echo "   Target: 10M, 25M, 50M, 75M, 100M       "
echo "   Started: $(date)                       "
echo "=========================================="

# -------------------- CONFIGURATION --------------------
# Paths
JAR="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/api/target/api-0.0.1-SNAPSHOT-shaded.jar"
BASE_DIR="/mnt/data/mehran/Datasets/SIFT1B"
CONFIG_DIR="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/config/src/main/resources"
OUTPUT_ROOT="/mnt/data/mehran/SCALABILITY_RUNS"

# Experiment Inputs
# We use the 10m config for ALL runs to keep parameters constant (scientific control)
CONFIG_FILE="$CONFIG_DIR/config_sift1b_10m.json"
QUERY_FILE="$BASE_DIR/bigann_query_1k.bvecs"     # 1k queries to save time
GT_FILE="$BASE_DIR/bigann_query_groundtruth.ivecs" # 1B GT (Recall will be invalid)

# Parameters
DIMENSION=128
BATCH_SIZE=100000

# JVM Options (High RAM for 100M)
JVM_ARGS=(
  "-XX:+UseG1GC"
  "-XX:MaxGCPauseMillis=200"
  "-XX:+AlwaysPreTouch"
  "-Xms250g"                  # Fixed allocation to prevent resize jitter
  "-Xmx354g"
  "-Dfile.encoding=UTF-8"
  "-Dreenc.mode=end"          # Only re-encrypt at end if needed
)

# -------------------- SETUP --------------------
mkdir -p "$OUTPUT_ROOT"

die() { echo "❌ ERROR: $*" >&2; exit 1; }

[[ -f "$JAR" ]] || die "Missing JAR: $JAR"
[[ -f "$CONFIG_FILE" ]] || die "Missing config: $CONFIG_FILE"
[[ -f "$QUERY_FILE" ]] || die "Missing query file: $QUERY_FILE"

# -------------------- THE LOOP --------------------
# Define the subsets to run
SIZES=("10m" "25m" "50m" "75m" "100m")

TOTAL_START=$(date +%s)

for SIZE in "${SIZES[@]}"; do
    echo ""
    echo "######################################################"
    echo "▶ STARTING EXPERIMENT: SIFT1B-${SIZE}"
    echo "######################################################"

    # Construct file paths
    DATA_FILE="$BASE_DIR/bigann_learn_${SIZE}.bvecs"
    RUN_DIR="$OUTPUT_ROOT/SIFT1B-${SIZE}"
    LOG_FILE="$RUN_DIR/run.log"

    # Validation
    if [[ ! -f "$DATA_FILE" ]]; then
        echo "⚠️  Skipping ${SIZE}: Data file not found ($DATA_FILE)"
        continue
    fi

    # Prep Directory
    echo "  • Working Dir: $RUN_DIR"
    rm -rf "$RUN_DIR"
    mkdir -p "$RUN_DIR/results"

    # Execution
    START_TIME=$(date +%s)
    echo "  • Java Heap: 250G - 300G"
    echo "  • Executing..."

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

        # Extract Metrics from Log
        # We greedily grep for "Index complete" to find the raw indexing time
        INDEX_TIME=$(grep "Index complete | time=" "$LOG_FILE" | tail -n 1 | awk -F'=' '{print $2}' | tr -d 's')

        echo "✅ DONE: SIFT1B-${SIZE}"
        echo "  ----------------------------------------"
        echo "  • Duration:    ${MINS} min ($DURATION sec)"
        echo "  • Index Time:  ${INDEX_TIME:-N/A} sec"
        echo "  • Log:         $LOG_FILE"
        echo "  ----------------------------------------"

    else
        echo "❌ FAILED: SIFT1B-${SIZE}"
        echo "  • Check log: $LOG_FILE"
        # We continue to the next size even if one fails, unless it's a critical error?
        # Usually better to stop if 10M fails, but if 75M fails, maybe we want to try 100M?
        # Let's exit on failure to save time debugging.
        exit 1
    fi

    # Optional: Cooldown to let GC settle / disk buffer flush
    sleep 30
done

# -------------------- SUMMARY --------------------
TOTAL_END=$(date +%s)
TOTAL_DURATION=$((TOTAL_END - TOTAL_START))
HOURS=$((TOTAL_DURATION / 3600))

echo ""
echo "=========================================="
echo "SWEEP COMPLETE"
echo "Total Time: ${HOURS} hours"
echo "Results stored in: $OUTPUT_ROOT"
echo "=========================================="