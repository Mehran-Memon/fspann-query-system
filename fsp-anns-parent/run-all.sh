#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

# ============================
# FSP-ANN (multi-config, multi-dataset, multi-profile batch) — Linux
# - Option-C: LSH Disabled, Alpha-Based Stabilization
# - Selective Reencryption (end-of-run)
# - Config-family isolation for academic evaluation
# ============================

# ---- required paths ----
JarPath="/home/user/fspann-query-system/fsp-anns-parent/api/target/api-0.0.1-SNAPSHOT-shaded.jar"

# ---- CONFIG FAMILIES ----
CONFIGS=(
  "SIFT1M::/home/user/fspann-query-system/fsp-anns-parent/config/src/main/resources/config_sift1m.json"
  "GLOVE100::/home/user/fspann-query-system/fsp-anns-parent/config/src/main/resources/config_glove100.json"
  "REDCAPS::/home/user/fspann-query-system/fsp-anns-parent/config/src/main/resources/config_redcaps.json"
)

# ---- JVM system props ----
JvmArgs=(
  "-XX:+UseG1GC" "-XX:MaxGCPauseMillis=200" "-XX:+AlwaysPreTouch"
  "-Ddisable.exit=true"
  "-Dfile.encoding=UTF-8"
  "-Dreenc.mode=end"
  "-Dreenc.minTouched=5000"
  "-Dreenc.batchSize=2000"
  "-Dlog.progress.everyN=0"
  "-Dpaper.buildThreshold=2000000"
  "-Djava.security.egd=file:/dev/./urandom"
)

Batch="100000"
OnlyProfile=""
CleanPerRun="true"
QueryOnly="false"
RestoreVersion=""

die() { echo "Error: $*" >&2; exit 1; }

have_java() { command -v java >/dev/null 2>&1; }

safe_resolve() {
  local p="${1:-}" allow="${2:-false}"
  if [[ "$allow" == "true" && ! -e "$p" ]]; then printf "%s" "$p"; return 0; fi
  command -v realpath >/dev/null 2>&1 && realpath "$p" || printf "%s" "$p"
}

ensure_files() {
  [[ -f "$1" && -f "$2" && -f "$3" ]]
}

fast_delete_dir() {
  local d="$1"
  [[ -d "$d" ]] || return
  local e="/tmp/empty_$(uuidgen)"
  mkdir -p "$e"
  rsync -a --delete "$e/" "$d/" || true
  rm -rf "$d" "$e"
}

clean_run_metadata() {
  local r="$1"
  for p in metadata points results; do
    fast_delete_dir "$r/$p"
    mkdir -p "$r/$p"
  done
}

json_merge() {
  python3 - "$1" "$2" <<'PY'
import json,sys
a=json.loads(sys.argv[1]); b=json.loads(sys.argv[2])
def m(x,y):
    for k,v in y.items():
        if k in x and isinstance(x[k],dict) and isinstance(v,dict): m(x[k],v)
        else: x[k]=v
    return x
print(json.dumps(m(a,b),separators=(',',':')))
PY
}

DATASETS=(
  "SIFT1M::/mnt/data/datasets/SIFT1M/sift_base.fvecs::/mnt/data/datasets/SIFT1M/sift_query.fvecs::/mnt/data/datasets/SIFT1M/sift_query_groundtruth.ivecs::128"
  "glove-100::/mnt/data/datasets/glove-100/glove-100_base.fvecs::/mnt/data/datasets/glove-100/glove-100_query.fvecs::/mnt/data/datasets/glove-100/glove-100_groundtruth.ivecs::100"
  "RedCaps::/mnt/data/datasets/RedCaps/redcaps_base.fvecs::/mnt/data/datasets/RedCaps/redcaps_query.fvecs::/mnt/data/datasets/RedCaps/redcaps_groundtruth.ivecs::512"
)

have_java || die "Java missing"
[[ -f "$JarPath" ]] || die "Jar missing"

for cfg in "${CONFIGS[@]}"; do
  CFG_NAME="${cfg%%::*}"
  ConfigPath="${cfg##*::}"
  [[ -f "$ConfigPath" ]] || die "Config missing: $ConfigPath"

  OutRoot="/mnt/data/fsp-run/${CFG_NAME}"
  mkdir -p "$OutRoot"

  echo "========================================"
  echo "CONFIG FAMILY: $CFG_NAME"
  echo "========================================"

  cfg_json="$(cat "$ConfigPath")"
  base_json="$(jq -c '.base' <<<"$cfg_json")"

  for ds in "${DATASETS[@]}"; do
    Name="${ds%%::*}"
    rest="${ds#*::}"
    Base="${rest%%::*}"
    rest="${rest#*::}"
    Query="${rest%%::*}"
    rest="${rest#*::}"
    GT="${rest%%::*}"
    Dim="${rest##*::}"

    ensure_files "$Base" "$Query" "$GT" || continue

    datasetRoot="$OutRoot/$Name"
    mkdir -p "$datasetRoot"

    jq -c '.profiles[]' <<<"$cfg_json" | while read -r profile; do
      label="$(jq -r '.name' <<<"$profile")"
      ovr="$(jq -c '.overrides' <<<"$profile")"

      runDir="$datasetRoot/$label"
      mkdir -p "$runDir"
      [[ "$CleanPerRun" == "true" ]] && clean_run_metadata "$runDir"

      final="$(json_merge "$base_json" "$ovr")"
      final="$(jq -c --arg r "$runDir/results" --arg gt "$GT" '
        .output.resultsDir=$r
        | .ratio.source="gt"
        | .ratio.gtPath=$gt
        | .ratio.autoComputeGT=false
        | .ratio.allowComputeIfMissing=false
      ' <<<"$final")"

      echo "$final" > "$runDir/config.json"

      java "${JvmArgs[@]}" \
        -Dcli.dataset="$Name" \
        -Dcli.profile="$label" \
        -jar "$JarPath" \
        "$runDir/config.json" "$Base" "$Query" "$runDir/keys.blob" \
        "$Dim" "$runDir" "$GT" "$Batch" \
        | tee "$runDir/run.out.log"

    done
  done
done
