#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

# =========================
# FSP-ANN Multi-Dataset Runner (Linux)
# =========================

JarPath="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/api/target/api-0.0.1-SNAPSHOT-shaded.jar"
ConfigPath="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/config/src/main/resources/config_sift1m.json"
OutRoot="/mnt/data/mehran"
Batch=100000

JvmArgs=(
  "-XX:+UseG1GC"
  "-XX:MaxGCPauseMillis=200"
  "-XX:+AlwaysPreTouch"
  "-Xmx16g"
  "-Ddisable.exit=true"
  "-Dfile.encoding=UTF-8"
  "-Dreenc.mode=end"
  "-Dreenc.minTouched=5000"
  "-Dreenc.batchSize=2000"
  "-Dlog.progress.everyN=100000"
  "-Dpaper.buildThreshold=2000000"
)

die() {
  echo "ERROR: $*" >&2
  exit 1
}

ensure_file() {
  [[ -f "$1" ]] || die "File not found: $1"
}

command -v java >/dev/null || die "java not found"
command -v jq   >/dev/null || die "jq not found"

ensure_file "$JarPath"
ensure_file "$ConfigPath"

mkdir -p "$OutRoot"

# =========================
# DATASETS
# =========================
DATASET_NAMES=("SIFT1M" "glove-100" "RedCaps")
DATASET_DIMS=(128 100 512)
DATASET_BASE=(
  "/mnt/data/mehran/Datasets/SIFT1M/sift_base.fvecs"
  "/mnt/data/mehran/Datasets/glove-100/glove-100_base.fvecs"
  "/mnt/data/mehran/Datasets/redcaps/redcaps_base.fvecs"
)
DATASET_QUERY=(
  "/mnt/data/mehran/Datasets/SIFT1M/sift_query.fvecs"
  "/mnt/data/mehran/Datasets/glove-100/glove-100_query.fvecs"
  "/mnt/data/mehran/Datasets/redcaps/redcaps_query.fvecs"
)
DATASET_GT=(
  "/mnt/data/mehran/Datasets/SIFT1M/sift_query_groundtruth.ivecs"
  "/mnt/data/mehran/Datasets/glove-100/glove-100_groundtruth.ivecs"
  "/mnt/data/mehran/Datasets/redcaps/redcaps_query_groundtruth.ivecs"
)

# =========================
# LOAD BASE CONFIG
# =========================
CFG_JSON="$(cat "$ConfigPath")"
BASE_JSON="$(jq -c 'del(.profiles)' <<<"$CFG_JSON")"
PROFILE_COUNT="$(jq '.profiles | length' <<<"$CFG_JSON")"

[[ "$PROFILE_COUNT" -gt 0 ]] || die "No profiles found in config"

# =========================
# MAIN LOOP
# =========================
for idx in "${!DATASET_NAMES[@]}"; do
  ds="${DATASET_NAMES[$idx]}"
  dim="${DATASET_DIMS[$idx]}"
  base="${DATASET_BASE[$idx]}"
  query="${DATASET_QUERY[$idx]}"
  gt="${DATASET_GT[$idx]}"

  [[ -f "$base" ]] || { echo "Skipping $ds (missing base)"; continue; }
  [[ -f "$query" ]] || { echo "Skipping $ds (missing query)"; continue; }
  [[ -f "$gt" ]] || { echo "Skipping $ds (missing GT)"; continue; }

  echo ""
  echo "========================================"
  echo "DATASET: $ds (dim=$dim)"
  echo "========================================"

  ds_root="$OutRoot/$ds"
  mkdir -p "$ds_root"

  printf "%-25s | %-6s\n" "PROFILE" "STATUS"
  printf "%-25s-+-%-6s\n" "-------------------------" "------"

  for ((p=0; p<PROFILE_COUNT; p++)); do
    profile="$(jq -c ".profiles[$p]" <<<"$CFG_JSON")"
    label="$(jq -r '.name' <<<"$profile")"

    [[ -n "$label" ]] || continue

    run_dir="$ds_root/$label"
    mkdir -p "$run_dir/results"

    overrides="$(jq -c '.overrides // {}' <<<"$profile")"

    final_cfg="$(jq -n \
      --argjson base "$BASE_JSON" \
      --argjson ovr  "$overrides" \
      --arg resdir "$run_dir/results" \
      --arg gtpath "$gt" \
      '$base + $ovr
       | .output.resultsDir=$resdir
       | .ratio.source="gt"
       | .ratio.gtPath=$gtpath
       | .ratio.autoComputeGT=false
       | .ratio.allowComputeIfMissing=false
      ')"

    echo "$final_cfg" > "$run_dir/config.json"

    log="$run_dir/run.log"

    if java "${JvmArgs[@]}" \
        "-Dcli.dataset=$ds" \
        "-Dcli.profile=$label" \
        -jar "$JarPath" \
        "$run_dir/config.json" \
        "$base" \
        "$query" \
        "$run_dir/keys.blob" \
        "$dim" \
        "$run_dir" \
        "$gt" \
        "$Batch" \
        >"$log" 2>&1
    then
      status="OK"
    else
      status="FAIL"
    fi

    printf "%-25s | %-6s\n" "$label" "$status"
  done
done

echo ""
echo "ALL DATASETS COMPLETED"
