#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

# ============================================================
# SIFT1B 100M SCALABILITY EXPERIMENT
# ============================================================

echo "SIFT1B 100M Scalability (100× baseline)"
echo "Started: $(date)"
echo "=========================================="

# -------------------- PATHS --------------------
JAR="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/api/target/api-0.0.1-SNAPSHOT-shaded.jar"
CONFIG="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/config/src/main/resources/config_sift1b_100m.json"
BASE="/mnt/data/mehran/Datasets/SIFT1B/bigann_learn_50m.bvecs"
QUERY="/mnt/data/mehran/Datasets/SIFT1B/bigann_query_1k.bvecs"
GT="/mnt/data/mehran/Datasets/SIFT1B/bigann_query_groundtruth.ivecs"
OUTPUT="/mnt/data/mehran/SIFT1B-100M"
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
[[ -f "$CONFIG" ]] || die "Missing config: $CONFIG"
[[ -f "$BASE" ]] || die "Missing base: $BASE"
[[ -f "$QUERY" ]] || die "Missing query: $QUERY"
[[ -f "$GT" ]] || die "Missing groundtruth: $GT"

# Create output directory
rm -rf "$OUTPUT"
mkdir -p "$OUTPUT/results"

# -------------------- RUN ----------------------
START_TIME=$(date +%s)

java "${JVM_ARGS[@]}" -jar "$JAR" \
  "$CONFIG" \
  "$BASE" \
  "$QUERY" \
  "$OUTPUT/keys.blob" \
  "$DIMENSION" \
  "$OUTPUT" \
  "$GT" \
  "$BATCH_SIZE" \
  > "$OUTPUT/run.log" 2>&1

EXIT_CODE=$?
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
HOURS=$((DURATION / 3600))
MINS=$(((DURATION % 3600) / 60))

echo ""
echo "=========================================="
if [ $EXIT_CODE -eq 0 ]; then
  echo "✓ SIFT1B 100M COMPLETED SUCCESSFULLY"
  echo "Duration: ${HOURS}h ${MINS}m"
  echo "Completed: $(date)"

  # Check for results
  if [[ -f "$OUTPUT/results/summary.csv" ]]; then
    echo ""
    echo "Results available at: $OUTPUT/results/summary.csv"

    # Extract key metrics
    RECALL_100=$(awk -F',' 'NR==2 {print $23}' "$OUTPUT/results/summary.csv")
    ART_MS=$(awk -F',' 'NR==2 {print $13}' "$OUTPUT/results/summary.csv")
    RATIO=$(awk -F',' 'NR==2 {print $7}' "$OUTPUT/results/summary.csv")

    echo ""
    echo "Key Results:"
    echo "  Recall@100: $RECALL_100"
    echo "  ART: ${ART_MS}ms"
    echo "  Ratio: $RATIO"
  fi
else
  echo "✗ SIFT1B 100M FAILED (exit code: $EXIT_CODE)"
  echo "Check logs: $OUTPUT/run.log"
  exit $EXIT_CODE
fi

# -------------------- DECISION TIME ------------
echo ""
echo "=========================================="
echo "Decision Time for Additional Scales"
echo "=========================================="
echo "Current time: $(date)"
echo "Deadline: Monday 08:00"
echo ""

CURRENT_HOUR=$(date +%H)
CURRENT_MIN=$(date +%M)
CURRENT_TIME_MINS=$((10#$CURRENT_HOUR * 60 + 10#$CURRENT_MIN))
DEADLINE_MINS=$((8 * 60))  # 08:00 = 480 minutes

# If it's past midnight (next day), add 24 hours
if [ $CURRENT_TIME_MINS -lt 480 ]; then
  CURRENT_TIME_MINS=$((CURRENT_TIME_MINS + 1440))
fi

REMAINING_MINS=$((DEADLINE_MINS + 1440 - CURRENT_TIME_MINS))
REMAINING_HOURS=$((REMAINING_MINS / 60))
REMAINING_MINS_PART=$((REMAINING_MINS % 60))

echo "Remaining time: ${REMAINING_HOURS}h ${REMAINING_MINS_PART}m"
echo ""
echo "Decision Matrix:"
echo "  > 4h 30m: Run BOTH 10M + 25M (recommended) ⭐"
echo "  3h 30m - 4h 30m: Run 25M only"
echo "  2h - 3h 30m: Run 10M only"
echo "  < 2h: STOP, verify results and prepare paper"
echo ""

if [ $REMAINING_MINS -gt 270 ]; then
  echo "Recommendation: RUN 10M + 25M for complete scalability curve!"
  echo ""
  echo "Execute: ./run_sift1b_10m_25m_combined.sh"
elif [ $REMAINING_MINS -gt 210 ]; then
  echo "Recommendation: Run 25M only (safer)"
elif [ $REMAINING_MINS -gt 120 ]; then
  echo "Recommendation: Run 10M only (fastest)"
else
  echo "Recommendation: STOP HERE, use remaining time for verification"
fi

echo "=========================================="