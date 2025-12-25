#!/usr/bin/env bash
set -Eeuo pipefail

# =========================
# FSP-ANN MULTI-DATASET RUNNER
# =========================

JarPath="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/api/target/api-0.0.1-SNAPSHOT-shaded.jar"
OutRoot="/mnt/data/mehran"
Batch=100000
QUERY_LIMIT=200

JvmArgs=(
  "-XX:+UseG1GC"
  "-XX:MaxGCPauseMillis=200"
  "-XX:+AlwaysPreTouch"
  "-Xmx16g"
  "-Dfile.encoding=UTF-8"
  "-Dreenc.mode=end"
)

die() { echo "ERROR: $*" >&2; exit 1; }
ensure_file() { [[ -f "$1" ]] || die "File not found: $1"; }

command -v java >/dev/null || die "java not found"
command -v jq   >/dev/null || die "jq not found"
ensure_file "$JarPath"
mkdir -p "$OutRoot"

# ================= DATASETS =================

declare -A DATASET_CONFIG=(
  ["RedCaps"]="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/config/src/main/resources/config_redcaps.json"
  ["SIFT1M"]="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/config/src/main/resources/config_sift1m.json"
  ["glove-100"]="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/config/src/main/resources/config_glove100.json"
)

declare -A DATASET_DIM=(
  ["RedCaps"]=512
  ["SIFT1M"]=128
  ["glove-100"]=100
)

declare -A DATASET_BASE=(
  ["RedCaps"]="/mnt/data/mehran/Datasets/redcaps/redcaps_base.fvecs"
  ["SIFT1M"]="/mnt/data/mehran/Datasets/SIFT1M/sift_base.fvecs"
  ["glove-100"]="/mnt/data/mehran/Datasets/glove-100/glove-100_base.fvecs"
)

declare -A DATASET_QUERY=(
  ["RedCaps"]="/mnt/data/mehran/Datasets/redcaps/redcaps_query.fvecs"
  ["SIFT1M"]="/mnt/data/mehran/Datasets/SIFT1M/sift_query.fvecs"
  ["glove-100"]="/mnt/data/mehran/Datasets/glove-100/glove-100_query.fvecs"
)

declare -A DATASET_GT=(
  ["RedCaps"]="/mnt/data/mehran/Datasets/redcaps/redcaps_query_groundtruth.ivecs"
  ["SIFT1M"]="/mnt/data/mehran/Datasets/SIFT1M/sift_query_groundtruth.ivecs"
  ["glove-100"]="/mnt/data/mehran/Datasets/glove-100/glove-100_groundtruth.ivecs"
)

# ================= DEBUG SWITCHES =================
ONLY_DATASET="SIFT1M"
ONLY_PROFILE="M24_lambda3"

# ================= METRICS =================

extract_metrics() {
  local csv="$1/profiler_metrics.csv"
  [[ -f "$csv" ]] || { echo "NA,NA"; return; }

  python3 <<EOF
import pandas as pd
df = pd.read_csv("$csv")
art = (df['serverMs'] + df['clientMs']).mean()
ratio = df['ratio'].mean()
print(f"{art:.2f},{ratio:.4f}")
EOF
}

# ================= GLOBAL SUMMARY =================

GLOBAL_SUMMARY="$OutRoot/global_summary.csv"
echo "dataset,profile,ART_ms,AvgRatio" > "$GLOBAL_SUMMARY"

# ================= MAIN LOOP =================

DATASETS=(SIFT1M glove-100 RedCaps)
[[ -n "$ONLY_DATASET" ]] && DATASETS=("$ONLY_DATASET")

for ds in "${DATASETS[@]}"; do
  cfg="${DATASET_CONFIG[$ds]}"
  dim="${DATASET_DIM[$ds]}"
  base="${DATASET_BASE[$ds]}"
  query="${DATASET_QUERY[$ds]}"
  gt="${DATASET_GT[$ds]}"

  ensure_file "$cfg"; ensure_file "$base"; ensure_file "$query"; ensure_file "$gt"

  CFG_JSON="$(cat "$cfg")"
  BASE_JSON="$(jq -c 'del(.profiles)' <<<"$CFG_JSON")"

  # Validate ONLY_PROFILE once
  if [[ -n "$ONLY_PROFILE" ]]; then
    jq -e ".profiles[] | select(.name == \"$ONLY_PROFILE\")" <<<"$CFG_JSON" \
      >/dev/null || die "Profile $ONLY_PROFILE not found in $ds"
  fi

  ds_root="$OutRoot/$ds"
  mkdir -p "$ds_root"
  echo "profile,ART_ms,AvgRatio" > "$ds_root/dataset_summary.csv"

  ran_any_profile=false
  PROFILE_COUNT="$(jq '.profiles | length' <<<"$CFG_JSON")"

  for ((i=0; i<PROFILE_COUNT; i++)); do
    profile="$(jq -c ".profiles[$i]" <<<"$CFG_JSON")"
    name="$(jq -r '.name' <<<"$profile")"

    [[ -n "$ONLY_PROFILE" && "$name" != "$ONLY_PROFILE" ]] && continue
    ran_any_profile=true

    overrides="$(jq -c '.overrides // {}' <<<"$profile")"
    run_dir="$ds_root/$name"
    mkdir -p "$run_dir/results"
    rm -f "$run_dir/keys.blob"
    rm -rf "$run_dir/metadata"

    final_cfg="$(jq -n '
      def deepmerge(a; b):
        reduce (b | keys[]) as $k
          (a;
           if (a[$k]|type=="object" and b[$k]|type=="object"
           then .[$k]=deepmerge(a[$k];b[$k])
           else .[$k]=b[$k] end);
      deepmerge($base;$ovr)
      | .output.resultsDir=$resdir
      | .ratio.source="gt"
      | .ratio.gtPath=$gtpath
      | .ratio.autoComputeGT=false
    ' --argjson base "$BASE_JSON" --argjson ovr "$overrides" \
       --arg resdir "$run_dir/results" --arg gtpath "$gt")"

    echo "$final_cfg" > "$run_dir/config.json"

    log="$run_dir/run.log"

    java "${JvmArgs[@]}" \
      -Dcli.dataset="$ds" \
      -Dcli.profile="$name" \
      -Dquery.limit="$QUERY_LIMIT" \
      -jar "$JarPath" \
      "$run_dir/config.json" \
      "$base" "$query" "$run_dir/keys.blob" \
      "$dim" "$run_dir" "$gt" "$Batch" \
      >"$log" 2>&1 || {
        echo "FAILED: $ds / $name"
        tail -50 "$log"
        exit 1
      }

    metrics=$(extract_metrics "$run_dir/results")
    IFS=',' read -r art ratio <<<"$metrics"

    echo "$name,$art,$ratio" >> "$ds_root/dataset_summary.csv"
    echo "$ds,$name,$art,$ratio" >> "$GLOBAL_SUMMARY"

    printf "%-12s | ART=%8s ms | Ratio=%s\n" "$name" "$art" "$ratio"

    [[ -n "$ONLY_PROFILE" ]] && break
  done

  [[ "$ran_any_profile" == true ]] || die "No profile ran for $ds"
done
