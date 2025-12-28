#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

# ============================================================
# FSP-ANN MULTI-DATASET SWEEP RUNNER (PAPER-GRADE, JAVA-ONLY)
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
  "-Xmx16g"
  "-Dfile.encoding=UTF-8"
  "-Dreenc.mode=end"
)

# -------------------- SAFETY -------------------
die() { echo "ERROR: $*" >&2; exit 1; }
need_file() { [[ -f "$1" ]] || die "Missing file: $1"; }
need_cmd() { command -v "$1" >/dev/null || die "Missing command: $1"; }

need_cmd java
need_cmd jq
need_cmd jshell
need_file "$JAR"

mkdir -p "$OUT_ROOT"

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
ONLY_DATASET="SIFT1M"
ONLY_PROFILE="M24"

# ================= METRIC EXTRACTION =================

extract_metrics() {
  local csv="$1/profiler_metrics.csv"
  [[ -f "$csv" ]] || { echo "NA,NA"; return; }

  jshell --execution local <<EOF 2>/dev/null | grep -E '^[0-9]'
import java.nio.file.*;
import java.util.*;

var lines = Files.readAllLines(Path.of("$csv"));
if (lines.size() <= 1) {
  System.out.println("NA,NA");
  System.exit(0);
}

var header = lines.get(0).split(",");
int clientIdx = -1, ratioIdx = -1;

for (int i = 0; i < header.length; i++) {
  if (header[i].equals("clientMs")) clientIdx = i;
  if (header[i].equals("ratio")) ratioIdx = i;
}

if (clientIdx < 0 || ratioIdx < 0) {
  System.out.println("NA,NA");
  System.exit(0);
}

double artSum = 0.0, ratioSum = 0.0;
int n = 0;

for (int i = 1; i < lines.size(); i++) {
  var p = lines.get(i).split(",");
  if (p.length <= Math.max(clientIdx, ratioIdx)) continue;
  artSum += Double.parseDouble(p[clientIdx]);
  ratioSum += Double.parseDouble(p[ratioIdx]);
  n++;
}

if (n == 0) {
  System.out.println("NA,NA");
} else {
  System.out.printf("%.2f,%.4f%n", artSum / n, ratioSum / n);
}
EOF
}

# ================= GLOBAL SUMMARY =================

GLOBAL_SUMMARY="$OUT_ROOT/global_summary.csv"
echo "dataset,profile,ART_ms,AvgRatio" > "$GLOBAL_SUMMARY"

# ================= MAIN LOOP ======================

DATASETS=(SIFT1M glove-100 RedCaps)
[[ -n "$ONLY_DATASET" ]] && DATASETS=("$ONLY_DATASET")

for ds in "${DATASETS[@]}"; do
  cfg="${CFG[$ds]}"
  dim="${DIM[$ds]}"
  base="${BASE[$ds]}"
  query="${QUERY[$ds]}"
  gt="${GT[$ds]}"

  need_file "$cfg"
  need_file "$base"
  need_file "$query"
  need_file "$gt"

  BASE_JSON="$(jq -c 'del(.profiles)' "$cfg")"

  ds_root="$OUT_ROOT/$ds"
  mkdir -p "$ds_root"
  echo "profile,ART_ms,AvgRatio" > "$ds_root/dataset_summary.csv"

  PROFILE_COUNT="$(jq '.profiles | length' "$cfg")"
  ran_any=false

  for ((i=0; i<PROFILE_COUNT; i++)); do
    prof="$(jq -c ".profiles[$i]" "$cfg")"
    name="$(jq -r '.name' <<<"$prof")"

    [[ -n "$ONLY_PROFILE" && "$name" != "$ONLY_PROFILE" ]] && continue
    ran_any=true

    overrides="$(jq -c '.overrides // {}' <<<"$prof")"
    run_dir="$ds_root/$name"

    rm -rf "$run_dir"
    mkdir -p "$run_dir/results"

    final_cfg="$(jq -n '
      def deepmerge(a; b):
        reduce (b | keys_unsorted[]) as $k
          (a;
           .[$k] =
             if (a[$k] | type) == "object" and (b[$k] | type) == "object"
             then deepmerge(a[$k]; b[$k])
             else b[$k]
             end);
      deepmerge($base; $ovr)
      | .output.resultsDir = $resdir
      | .ratio.source = "gt"
      | .ratio.gtPath = $gtpath
      | .ratio.allowComputeIfMissing = false
    ' \
      --argjson base "$BASE_JSON" \
      --argjson ovr "$overrides" \
      --arg resdir "$run_dir/results" \
      --arg gtpath "$gt"
    )"

    echo "$final_cfg" > "$run_dir/config.json"
    log="$run_dir/run.log"

    java "${JVM_ARGS[@]}" \
      -Dcli.dataset="$ds" \
      -Dcli.profile="$name" \
      -jar "$JAR" \
      "$run_dir/config.json" \
      "$base" "$query" "$run_dir/keys.blob" \
      "$dim" "$run_dir" "$gt" "$BATCH_SIZE" \
      >"$log" 2>&1 || {
        echo "FAILED: $ds / $name"
        tail -80 "$log"
        exit 1
      }

    metrics="$(extract_metrics "$run_dir/results")"
    IFS=',' read -r art ratio <<<"$metrics"

    echo "$name,$art,$ratio" >> "$ds_root/dataset_summary.csv"
    echo "$ds,$name,$art,$ratio" >> "$GLOBAL_SUMMARY"

    printf "%-10s | %-12s | ART=%7s ms | Ratio=%s\n" \
      "$ds" "$name" "${art:-NA}" "${ratio:-NA}"
  done

  [[ "$ran_any" == true ]] || die "No profile executed for dataset $ds"
done
