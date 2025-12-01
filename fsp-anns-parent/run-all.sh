#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

# ============================
# FSP-ANN — Linux Runner (Option A)
# One config file per dataset (config_<dataset>.json)
# ============================

JarPath="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/api/target/api-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
ConfigDir="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/config/src/main/resources"
OutRoot="/mnt/data/mehran/fsp-ann-results"

Alpha="0.1"
Batch="100000"

# JVM settings (unchanged)
JvmArgs=(
  "-XX:+UseG1GC"
  "-XX:MaxGCPauseMillis=200"
  "-XX:+AlwaysPreTouch"
  "-Ddisable.exit=true"
  "-Dfile.encoding=UTF-8"
  "-Dreenc.mode=end"
  "-Dreenc.minTouched=5000"
  "-Dreenc.batchSize=2000"
  "-Dlog.progress.everyN=0"
  "-Dpaper.buildThreshold=2000000"
  "-Djava.security.egd=file:/dev/./urandom"
  "-Dpaper.alpha=${Alpha}"
)

# ----------------------------
# Hard-coded dataset mapping
# (this replaces .datasets[] in config)
# ----------------------------

declare -A DATASET_BASE
declare -A DATASET_QUERY
declare -A DATASET_GT
declare -A DATASET_DIM

DATASET_BASE["sift1m"]="/mnt/data/mehran/Datasets/SIFT1M/sift_base.fvecs"
DATASET_QUERY["sift1m"]="/mnt/data/mehran/Datasets/SIFT1M/sift_query.fvecs"
DATASET_GT["sift1m"]="/mnt/data/mehran/Datasets/SIFT1M/sift_query_groundtruth.ivecs"
DATASET_DIM["sift1m"]="128"

DATASET_BASE["glove100"]="/mnt/data/mehran/Datasets/glove-100/glove-100_base.fvecs"
DATASET_QUERY["glove100"]="/mnt/data/mehran/Datasets/glove-100/glove-100_query.fvecs"
DATASET_GT["glove100"]="/mnt/data/mehran/Datasets/glove-100/glove-100_groundtruth.ivecs"
DATASET_DIM["glove100"]="100"

DATASET_BASE["redcaps"]="/mnt/data/mehran/Datasets/redcaps/redcaps_base.fvecs"
DATASET_QUERY["redcaps"]="/mnt/data/mehran/Datasets/redcaps/redcaps_query.fvecs"
DATASET_GT["redcaps"]="/mnt/data/mehran/Datasets/redcaps/redcaps_query_groundtruth.ivecs"
DATASET_DIM["redcaps"]="512"

#DATASET_BASE["deep1b"]="/mnt/data/Datasets/Deep1B/deep1b_base.fvecs"
#DATASET_QUERY["deep1b"]="/mnt/data/Datasets/Deep1B/deep1b_query.fvecs"
#DATASET_GT["deep1b"]="/mnt/data/Datasets/Deep1B/deep1b_groundtruth.ivecs"
#DATASET_DIM["deep1b"]="96"
#
#DATASET_BASE["gist1b"]="/mnt/data/Datasets/GIST1B/gist1b_base.fvecs"
#DATASET_QUERY["gist1b"]="/mnt/data/Datasets/GIST1B/gist1b_query.fvecs"
#DATASET_GT["gist1b"]="/mnt/data/Datasets/GIST1B/gist1b_groundtruth.ivecs"
#DATASET_DIM["gist1b"]="960"

# =================================
# Helper Functions (unchanged)
# =================================

die() { echo "Error: $*" >&2; exit 1; }

safe_resolve() {
  local p="${1:-}" allow="${2:-false}"
  if [[ "$allow" == "true" && ! -e "$p" ]]; then printf "%s" "$p"; return 0; fi
  realpath "$p" 2>/dev/null || printf "%s" "$p"
}

fast_delete_dir() {
  local target="$1"
  [[ -d "$target" ]] || { rm -f "$target"; return 0; }
  local empty="/tmp/empty_$(uuidgen)"
  mkdir -p "$empty"
  rsync -a --delete "$empty/" "$target/" || true
  rm -rf "$target" "$empty"
}

clean_run_metadata() {
  local dir="$1"
  for p in "$dir/metadata" "$dir/points" "$dir/results"; do
    [[ -e "$p" ]] && fast_delete_dir "$p"
    mkdir -p "$p"
  done
}

profile_from_path() {
  local p="$(dirname "$1")"
  basename "$(dirname "$p")"
}

combine_csv_with_profile() {
  local out="$1"; shift
  local col="$1"; shift
  local header="false"
  mkdir -p "$(dirname "$out")"; rm -f "$out"
  for f in "$@"; do
    [[ -f "$f" ]] || continue
    local prof="$(profile_from_path "$f")"
    if [[ "$header" == "false" ]]; then
      echo "${col},$(head -n1 "$f")" > "$out"
      tail -n +2 "$f" | awk -v p="$prof" 'NF{print p","$0}' >> "$out"
      header="true"
    else
      tail -n +2 "$f" | awk -v p="$prof" 'NF{print p","$0}' >> "$out"
    fi
  done
}

concat_txt_with_profile() {
  local out="$1"; shift
  mkdir -p "$(dirname "$out")"; rm -f "$out"
  for f in "$@"; do
    [[ -f "$f" ]] || continue
    local prof="$(profile_from_path "$f")"
    echo "===== PROFILE: ${prof} =====" >> "$out"
    cat "$f" >> "$out"
    echo >> "$out"
  done
}

json_merge_with_overrides() {
  local base="$1" ovr="$2"
  jq -c --argjson ovr "$ovr" '
    def merge(a;b):
      reduce (b|keys[]) as $k (a;
        if (a[$k]|type=="object") and (b[$k]|type=="object")
        then .[$k] = merge(a[$k]; b[$k])
        else .[$k] = b[$k]
        end
      );
    merge(.; $ovr)
  ' <<< "$base"
}

# ====================================
# MAIN EXECUTION LOOP
# ====================================

mkdir -p "$OutRoot"

echo "[INFO] Detecting config_* in $ConfigDir"
mapfile -t CONFIGS < <(ls "$ConfigDir"/config_*.json 2>/dev/null)

[[ "${#CONFIGS[@]}" -gt 0 ]] || die "No config_*.json found."

for cfg in "${CONFIGS[@]}"; do
  baseCfg="$(basename "$cfg")"
  datasetId="${baseCfg#config_}"
  datasetId="${datasetId%.json}"
  datasetKey="$(echo "$datasetId" | tr '[:upper:]' '[:lower:]')"

  [[ -n "${DATASET_BASE[$datasetKey]:-}" ]] || die "No dataset mapping for $datasetKey"

  Base="${DATASET_BASE[$datasetKey]}"
  Query="${DATASET_QUERY[$datasetKey]}"
  GT="${DATASET_GT[$datasetKey]}"
  Dim="${DATASET_DIM[$datasetKey]}"

  echo
  echo "===================================================="
  echo "[CONFIG] $cfg"
  echo "[DATASET] $datasetKey"
  echo "===================================================="

  cfg_json="$(cat "$cfg")"
  profiles_count="$(jq '.profiles|length' <<<"$cfg_json")"
  [[ "$profiles_count" -gt 0 ]] || die "Config has no profiles: $cfg"

  base_json="$(jq -c '.base' <<<"$cfg_json")"

  datasetRoot="${OutRoot}/${datasetKey}"
  mkdir -p "$datasetRoot"

  # Run each profile
  jq -c '.profiles[]' <<<"$cfg_json" | while read -r pr; do
    label="$(jq -r '.id' <<<"$pr")"
    runDir="$datasetRoot/$label"
    mkdir -p "$runDir"
    clean_run_metadata "$runDir"

ovr_json="$(jq -c 'del(.id)' <<<"$pr")"
final_json="$(jq -c --argjson base "$base_json" --argjson ovr "$ovr_json" '
  $base
  | .paper = (.paper + $ovr.paper)
' <<< "{}")"


    final_json="$(jq -c --arg rd "$runDir/results" --arg gtPath "$GT" '
      .output.resultsDir=$rd
      | .ratio.source="gt"
      | .ratio.gtPath=$gtPath
      | .ratio.autoComputeGT=false
      | .ratio.allowComputeIfMissing=false
    ' <<<"$final_json")"

    confFile="$runDir/config.json"
    echo "$final_json" > "$confFile"

    keysFile="$runDir/keystore.blob"

    javaArgs=(
      "${JvmArgs[@]}"
      "-Dbase.path=$(safe_resolve "$Base")"
      "-jar" "$(safe_resolve "$JarPath")"
      "$confFile"
      "$Base"
      "$Query"
      "$keysFile"
      "$Dim"
      "$runDir"
      "$GT"
      "$Batch"
    )

    echo "java ${javaArgs[*]}" > "$runDir/cmdline.txt"

    echo "[RUN] $datasetKey / $label"
    java "${javaArgs[@]}" 2>&1 | tee "$runDir/run.out.log"

  done

  # Merge results
  echo "[MERGE] $datasetKey"

  mapfile -t g_results < <(find "$datasetRoot" -path "*/results/results_table.csv")
  mapfile -t g_prec    < <(find "$datasetRoot" -path "*/results/global_precision.csv")
  mapfile -t g_topk    < <(find "$datasetRoot" -path "*/results/topk_evaluation.csv")
  mapfile -t g_reenc   < <(find "$datasetRoot" -path "*/results/reencrypt_metrics.csv")
  mapfile -t g_samples < <(find "$datasetRoot" -path "*/results/retrieved_samples.csv")
  mapfile -t g_worst   < <(find "$datasetRoot" -path "*/results/retrieved_worst.csv")
  mapfile -t g_stor    < <(find "$datasetRoot" -path "*/results/storage_summary.csv")
  mapfile -t g_mtxt    < <(find "$datasetRoot" -path "*/results/metrics_summary.txt")
  mapfile -t g_btxt    < <(find "$datasetRoot" -path "*/results/storage_breakdown.txt")

  combine_csv_with_profile "$datasetRoot/combined_results.csv" "profile" "${g_results[@]}"
  combine_csv_with_profile "$datasetRoot/combined_precision.csv" "profile" "${g_prec[@]}"
  combine_csv_with_profile "$datasetRoot/combined_topk.csv"      "profile" "${g_topk[@]}"
  combine_csv_with_profile "$datasetRoot/combined_reencrypt.csv" "profile" "${g_reenc[@]}"
  combine_csv_with_profile "$datasetRoot/combined_samples.csv"   "profile" "${g_samples[@]}"
  combine_csv_with_profile "$datasetRoot/combined_worst.csv"     "profile" "${g_worst[@]}"
  combine_csv_with_profile "$datasetRoot/combined_storage_summary.csv" "profile" "${g_stor[@]}"

  concat_txt_with_profile "$datasetRoot/combined_metrics_summary.txt" "${g_mtxt[@]}"
  concat_txt_with_profile "$datasetRoot/combined_storage_breakdown.txt" "${g_btxt[@]}"

done
