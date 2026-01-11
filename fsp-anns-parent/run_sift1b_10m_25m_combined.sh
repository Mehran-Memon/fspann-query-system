#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

# ============================================================
# SIFT1B 10M + 25M COMBINED SCALABILITY RUN
# ============================================================

echo "=========================================="
echo "SIFT1B Multi-Scale Experiment"
echo "Running: 10M (10×) + 25M (25×) sequentially"
echo "Started: $(date)"
echo "=========================================="

# -------------------- PATHS --------------------
JAR="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/api/target/api-0.0.1-SNAPSHOT-shaded.jar"
BASE_DIR="/mnt/data/mehran/Datasets/SIFT1B"
CONFIG_DIR="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/config/src/main/resources"
OUTPUT_ROOT="/mnt/data/mehran"

QUERY="$BASE_DIR/bigann_query_1k.bvecs"
GT="$BASE_DIR/bigann_query_groundtruth.ivecs"
DIMENSION=128
BATCH_SIZE=100000

# -------------------- JVM ----------------------
JVM_ARGS=(
  "-XX:+UseG1GC"
  "-XX:MaxGCPauseMillis=200"
  "-XX:+AlwaysPreTouch"
  "-Xms320g"
  "-Xmx354g"
  "-Xmn128g"
  "-Dfile.encoding=UTF-8"
  "-Dreenc.mode=end"
)

# -------------------- SAFETY -------------------
die() { echo "ERROR: $*" >&2; exit 1; }

[[ -f "$JAR" ]] || die "Missing JAR: $JAR"
[[ -f "$QUERY" ]] || die "Missing query: $QUERY"
[[ -f "$GT" ]] || die "Missing groundtruth: $GT"

# -------------------- TRACKING -----------------
TOTAL_START=$(date +%s)
FAILED_RUNS=()

# ============================================================
# EXPERIMENT 1: SIFT1B 10M
# ============================================================

echo ""
echo "=========================================="
echo "[1/2] SIFT1B 10M (10× baseline)"
echo "=========================================="

CONFIG_10M="$CONFIG_DIR/config_sift1b_10m.json"
BASE_10M="$BASE_DIR/bigann_learn_10m.bvecs"
OUTPUT_10M="$OUTPUT_ROOT/SIFT1B-10M"

[[ -f "$CONFIG_10M" ]] || die "Missing config: $CONFIG_10M"
[[ -f "$BASE_10M" ]] || die "Missing base: $BASE_10M"

rm -rf "$OUTPUT_10M"
mkdir -p "$OUTPUT_10M/results"

echo "Starting 10M at: $(date)"
START_10M=$(date +%s)

if java "${JVM_ARGS[@]}" -jar "$JAR" \
  "$CONFIG_10M" \
  "$BASE_10M" \
  "$QUERY" \
  "$OUTPUT_10M/keys.blob" \
  "$DIMENSION" \
  "$OUTPUT_10M" \
  "$GT" \
  "$BATCH_SIZE" \
  > "$OUTPUT_10M/run.log" 2>&1; then

  END_10M=$(date +%s)
  DURATION_10M=$((END_10M - START_10M))
  MINS_10M=$((DURATION_10M / 60))

  echo "✓ 10M COMPLETED in ${MINS_10M} minutes"

  if [[ -f "$OUTPUT_10M/results/summary.csv" ]]; then
    RECALL_10M=$(awk -F',' 'NR==2 {print $23}' "$OUTPUT_10M/results/summary.csv")
    ART_10M=$(awk -F',' 'NR==2 {print $13}' "$OUTPUT_10M/results/summary.csv")
    echo "  Recall@100: $RECALL_10M | ART: ${ART_10M}ms"
  fi
else
  echo "✗ 10M FAILED"
  FAILED_RUNS+=("10M")
fi

# ============================================================
# EXPERIMENT 2: SIFT1B 25M
# ============================================================

echo ""
echo "=========================================="
echo "[2/2] SIFT1B 25M (25× baseline)"
echo "=========================================="

CONFIG_25M="$CONFIG_DIR/config_sift1b_25m.json"
BASE_25M="$BASE_DIR/bigann_learn_25m.bvecs"
OUTPUT_25M="$OUTPUT_ROOT/SIFT1B-25M"

[[ -f "$CONFIG_25M" ]] || die "Missing config: $CONFIG_25M"
[[ -f "$BASE_25M" ]] || die "Missing base: $BASE_25M"

rm -rf "$OUTPUT_25M"
mkdir -p "$OUTPUT_25M/results"

echo "Starting 25M at: $(date)"
START_25M=$(date +%s)

if java "${JVM_ARGS[@]}" -jar "$JAR" \
  "$CONFIG_25M" \
  "$BASE_25M" \
  "$QUERY" \
  "$OUTPUT_25M/keys.blob" \
  "$DIMENSION" \
  "$OUTPUT_25M" \
  "$GT" \
  "$BATCH_SIZE" \
  > "$OUTPUT_25M/run.log" 2>&1; then

  END_25M=$(date +%s)
  DURATION_25M=$((END_25M - START_25M))
  MINS_25M=$((DURATION_25M / 60))

  echo "✓ 25M COMPLETED in ${MINS_25M} minutes"

  if [[ -f "$OUTPUT_25M/results/summary.csv" ]]; then
    RECALL_25M=$(awk -F',' 'NR==2 {print $23}' "$OUTPUT_25M/results/summary.csv")
    ART_25M=$(awk -F',' 'NR==2 {print $13}' "$OUTPUT_25M/results/summary.csv")
    echo "  Recall@100: $RECALL_25M | ART: ${ART_25M}ms"
  fi
else
  echo "✗ 25M FAILED"
  FAILED_RUNS+=("25M")
fi

# ============================================================
# FINAL SUMMARY
# ============================================================

TOTAL_END=$(date +%s)
TOTAL_DURATION=$((TOTAL_END - TOTAL_START))
TOTAL_HOURS=$((TOTAL_DURATION / 3600))
TOTAL_MINS=$(((TOTAL_DURATION % 3600) / 60))

echo ""
echo "=========================================="
echo "MULTI-SCALE EXPERIMENT COMPLETE"
echo "=========================================="
echo "Completed: $(date)"
echo "Total Duration: ${TOTAL_HOURS}h ${TOTAL_MINS}m"
echo ""

if [ ${#FAILED_RUNS[@]} -eq 0 ]; then
  echo "✓ ALL EXPERIMENTS SUCCESSFUL!"
  echo ""
  echo "Results:"
  echo "  10M: $OUTPUT_10M/results/summary.csv"
  echo "  25M: $OUTPUT_25M/results/summary.csv"
  echo ""
  echo "You now have: 1M, 10M, 25M, 100M scalability data!"
  echo "⭐ EXCELLENT: 4-point scalability curve achieved!"
else
  echo "⚠️  Some experiments failed:"
  for fail in "${FAILED_RUNS[@]}"; do
    echo "  - $fail"
  done
fi

echo "=========================================="