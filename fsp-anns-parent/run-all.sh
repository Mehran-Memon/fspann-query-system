#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

# ============================================================
# FSP-ANN MULTI-DATASET SWEEP RUNNER (PRODUCTION, PAPER-GRADE)
# ============================================================

# -------------------- PATHS --------------------
JAR="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/api/target/api-0.0.1-SNAPSHOT-shaded.jar"
OUT_ROOT="/mnt/data/mehran"
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
need_file() { [[ -f "$1" ]] || die "Missing file: $1"; }
need_cmd() { command -v "$1" >/dev/null || die "Missing command: $1"; }

need_cmd java
need_cmd jq
need_file "$JAR"

mkdir -p "$OUT_ROOT"

# Check disk space
AVAILABLE_GB=$(df -BG "$OUT_ROOT" | tail -1 | awk '{print $4}' | sed 's/G//')
REQUIRED_GB=60
if [ "$AVAILABLE_GB" -lt "$REQUIRED_GB" ]; then
  die "Insufficient disk space: ${AVAILABLE_GB}GB available, ${REQUIRED_GB}GB required"
fi
echo "Disk space: ${AVAILABLE_GB}GB available"

# ================= DATASETS ====================

declare -A CFG=(
  ["SIFT1M"]="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/config/src/main/resources/config_sift1m.json"
  ["glove-100"]="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/config/src/main/resources/config_glove100.json"
  ["RedCaps"]="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/config/src/main/resources/config_redcaps.json"
)

declare -A DIM=(
  ["SIFT1M"]=128
  ["glove-100"]=100
  ["RedCaps"]=512
)

declare -A BASE=(
  ["SIFT1M"]="/mnt/data/mehran/Datasets/SIFT1M/sift_base.fvecs"
  ["glove-100"]="/mnt/data/mehran/Datasets/glove-100/glove-100_base.fvecs"
  ["RedCaps"]="/mnt/data/mehran/Datasets/redcaps/redcaps_base.fvecs"
)

declare -A QUERY=(
  ["SIFT1M"]="/mnt/data/mehran/Datasets/SIFT1M/sift_query.fvecs"
  ["glove-100"]="/mnt/data/mehran/Datasets/glove-100/glove-100_query.fvecs"
  ["RedCaps"]="/mnt/data/mehran/Datasets/redcaps/redcaps_query.fvecs"
)

declare -A GT=(
  ["SIFT1M"]="/mnt/data/mehran/Datasets/SIFT1M/sift_query_groundtruth.ivecs"
  ["glove-100"]="/mnt/data/mehran/Datasets/glove-100/glove-100_groundtruth.ivecs"
  ["RedCaps"]="/mnt/data/mehran/Datasets/redcaps/redcaps_query_groundtruth.ivecs"
)

# ================= FILTERS =====================
ONLY_DATASET=""
ONLY_PROFILE=""

# ================= TRACKING ====================
FAILED_PROFILES=()
SWEEP_START=$(date +%s)

echo "SWEEP STARTED: $(date)"
echo "Datasets: SIFT1M-128, Glove-100, RedCaps-512"
echo "Profiles per dataset: 9"
echo "Total profiles: 27"
echo ""

# ================= GLOBAL SUMMARY =================

GLOBAL_SUMMARY="$OUT_ROOT/global_summary.csv"
echo "dataset,profile,ART_ms,AvgRatio,recall_at_100,ratio@20,ratio@40,ratio@60,ratio@80,ratio@100" \
  > "$GLOBAL_SUMMARY"

# ================= MAIN LOOP ======================

DATASETS=(SIFT1M glove-100 RedCaps)
[[ -n "$ONLY_DATASET" ]] && DATASETS=("$ONLY_DATASET")

for ds in "${DATASETS[@]}"; do
  echo "DATASET: $ds"

  cfg="${CFG[$ds]}"
  dim="${DIM[$ds]}"
  base="${BASE[$ds]}"
  query="${QUERY[$ds]}"
  gt="${GT[$ds]}"

  # Validate config exists and has profiles
  [[ -f "$cfg" ]] || die "Config not found: $cfg"
  PROFILE_COUNT="$(jq '.profiles | length' "$cfg")"
  [[ "$PROFILE_COUNT" -gt 0 ]] || die "No profiles in config: $cfg"
  echo "  Profiles to run: $PROFILE_COUNT"

  BASE_JSON="$(jq -c 'del(.profiles)' "$cfg")"

  ds_root="$OUT_ROOT/$ds"
  mkdir -p "$ds_root"

  echo "dataset,profile,ART_ms,AvgRatio,recall_at_100,ratio@20,ratio@40,ratio@60,ratio@80,ratio@100" \
    > "$ds_root/dataset_summary.csv"

  for ((i=0;i<PROFILE_COUNT;i++)); do
    prof="$(jq -c ".profiles[$i]" "$cfg")"
    name="$(jq -r '.name' <<<"$prof")"

    [[ -n "$ONLY_PROFILE" && "$name" != "$ONLY_PROFILE" ]] && continue

    echo ""
    echo "[$ds] Running profile $((i+1))/$PROFILE_COUNT: $name"

    overrides="$(jq -c '.overrides // {}' <<<"$prof")"
    run_dir="$ds_root/$name"

    rm -rf "$run_dir"
    mkdir -p "$run_dir/results"

    final_cfg="$(jq -n '
      def deepmerge(a;b):
        reduce (b|keys_unsorted[]) as $k (a;
          .[$k]=(if (a[$k]|type)=="object" and (b[$k]|type)=="object"
                 then deepmerge(a[$k];b[$k]) else b[$k] end));
      deepmerge($base;$ovr)
      | .output.resultsDir=$res
      | .ratio.source="gt"
      | .ratio.gtPath=$gt
      | .ratio.allowComputeIfMissing=false
    ' --argjson base "$BASE_JSON" \
      --argjson ovr "$overrides" \
      --arg res "$run_dir/results" \
      --arg gt "$gt")"

    echo "$final_cfg" > "$run_dir/config.json"
    sha256sum "$run_dir/config.json" > "$run_dir/config.sha256"

    start_ts=$(date +%s)

    if ! java "${JVM_ARGS[@]}" -jar "$JAR" \
      "$run_dir/config.json" "$base" "$query" "$run_dir/keys.blob" \
      "$dim" "$run_dir" "$gt" "$BATCH_SIZE" \
      >"$run_dir/run.log" 2>&1; then
        echo "  ❌ FAILED: $ds | $name"
        FAILED_PROFILES+=("$ds/$name")
        continue
    fi

    end_ts=$(date +%s)
    duration=$((end_ts - start_ts))
    echo "  ✓ COMPLETED in ${duration}s"

    acc="$run_dir/results/summary.csv"
    if [[ ! -f "$acc" ]]; then
      echo "  ⚠️  WARNING: Missing summary.csv"
      FAILED_PROFILES+=("$ds/$name (no summary)")
      continue
    fi

    # Validate CSV has enough columns
    COL_COUNT=$(head -1 "$acc" | awk -F',' '{print NF}')
    if [ "$COL_COUNT" -lt 28 ]; then
      echo "  ⚠️  WARNING: CSV has only $COL_COUNT columns (expected 28+)"
      FAILED_PROFILES+=("$ds/$name (invalid CSV)")
      continue
    fi

    avg_art=$(awk -F',' 'NR>1 {print $13}' "$acc")
    avg_ratio=$(awk -F',' 'NR>1 {print $7}' "$acc")
    recall_100=$(awk -F',' 'NR>1 {print $23}' "$acc")
    ratio20=$(awk -F',' 'NR>1 {print $24}' "$acc")
    ratio40=$(awk -F',' 'NR>1 {print $25}' "$acc")
    ratio60=$(awk -F',' 'NR>1 {print $26}' "$acc")
    ratio80=$(awk -F',' 'NR>1 {print $27}' "$acc")
    ratio100=$(awk -F',' 'NR>1 {print $28}' "$acc")

    echo "$ds,$name,$avg_art,$avg_ratio,$recall_100,$ratio20,$ratio40,$ratio60,$ratio80,$ratio100" \
      >> "$ds_root/dataset_summary.csv"

    echo "$ds,$name,$avg_art,$avg_ratio,$recall_100,$ratio20,$ratio40,$ratio60,$ratio80,$ratio100" \
      >> "$GLOBAL_SUMMARY"

    echo "  Recall@100: $recall_100 | ART: ${avg_art}ms"
  done

  echo ""
  echo "✓ $ds completed"
done

# ================= FINAL SUMMARY =================

SWEEP_END=$(date +%s)
SWEEP_DURATION=$((SWEEP_END - SWEEP_START))
SWEEP_HOURS=$((SWEEP_DURATION / 3600))
SWEEP_MINS=$(((SWEEP_DURATION % 3600) / 60))

echo ""
echo "SWEEP COMPLETED: $(date)"
echo "Total Duration: ${SWEEP_HOURS}h ${SWEEP_MINS}m"

if [ ${#FAILED_PROFILES[@]} -gt 0 ]; then
  echo ""
  echo "⚠️  FAILED PROFILES (${#FAILED_PROFILES[@]}):"
  for fail in "${FAILED_PROFILES[@]}"; do
    echo "  - $fail"
  done
else
  echo ""
  echo "ll profiles completed successfully!"
fi

echo ""
echo "Results:"
echo "  Global summary: $GLOBAL_SUMMARY"
echo "  Per-dataset: $OUT_ROOT/{SIFT1M,glove-100,RedCaps}/dataset_summary.csv"
echo ""