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
  "-Xmx24g"
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
ONLY_DATASET=""
ONLY_PROFILE=""

# ================= K VARIANTS ==================
KLIST="1,5,10,20,40,60,80,100"

# ================= METRIC EXTRACTION =================

extract_metrics() {
  local csv="$1/profiler_metrics.csv"
  [[ -f "$csv" ]] || { echo "NA,NA"; return; }

  jshell --execution local <<EOF 2>/dev/null | grep -E '^[0-9]'
import java.nio.file.*;
import java.util.*;

var lines = Files.readAllLines(Path.of("$csv"));
if (lines.size() <= 1) { System.out.println("NA,NA"); System.exit(0); }

var header = lines.get(0).split(",");
int clientIdx=-1, ratioIdx=-1;
for (int i=0;i<header.length;i++){
  if(header[i].equals("clientMs")) clientIdx=i;
  if(header[i].equals("ratio")) ratioIdx=i;
}

double art=0, r=0; int n=0;
for(int i=1;i<lines.size();i++){
  var p=lines.get(i).split(",");
  art+=Double.parseDouble(p[clientIdx]);
  r+=Double.parseDouble(p[ratioIdx]);
  n++;
}
System.out.printf("%.2f,%.4f%n", art/n, r/n);
EOF
}

extract_ratio_per_k() {
  local csv="$1/profiler_metrics.csv"
  local klist="$2"
  [[ -f "$csv" ]] || { echo ""; return; }

  jshell --execution local <<EOF 2>/dev/null | grep -E '^[0-9]'
import java.nio.file.*;
import java.util.*;

var ks=new TreeSet<Integer>();
for(var s:"$klist".split(",")) ks.add(Integer.parseInt(s));

var lines=Files.readAllLines(Path.of("$csv"));
var h=lines.get(0).split(",");
int kI=-1,rI=-1;
for(int i=0;i<h.length;i++){ if(h[i].equals("k"))kI=i; if(h[i].equals("ratio"))rI=i;}

var sum=new HashMap<Integer,Double>();
var cnt=new HashMap<Integer,Integer>();
for(int k:ks){sum.put(k,0.0);cnt.put(k,0);}

for(int i=1;i<lines.size();i++){
 var p=lines.get(i).split(",");
 int k=Integer.parseInt(p[kI]);
 if(!ks.contains(k))continue;
 sum.put(k,sum.get(k)+Double.parseDouble(p[rI]));
 cnt.put(k,cnt.get(k)+1);
}

for(int k:ks){
 if(cnt.get(k)==0) System.out.print("NA");
 else System.out.printf("%.4f",sum.get(k)/cnt.get(k));
 System.out.print(",");
}
System.out.println();
EOF
}

# ================= GLOBAL SUMMARY =================

GLOBAL_SUMMARY="$OUT_ROOT/global_summary.csv"
echo "dataset,profile,ART_ms,AvgRatio,ratio@1,ratio@5,ratio@10,ratio@20,ratio@40,ratio@60,ratio@80,ratio@100" \
  > "$GLOBAL_SUMMARY"

# ================= MAIN LOOP ======================

DATASETS=(SIFT1M glove-100 RedCaps)
[[ -n "$ONLY_DATASET" ]] && DATASETS=("$ONLY_DATASET")

for ds in "${DATASETS[@]}"; do
  cfg="${CFG[$ds]}"
  dim="${DIM[$ds]}"
  base="${BASE[$ds]}"
  query="${QUERY[$ds]}"
  gt="${GT[$ds]}"

  BASE_JSON="$(jq -c 'del(.profiles)' "$cfg")"

  ds_root="$OUT_ROOT/$ds"
  mkdir -p "$ds_root"
  echo "profile,ART_ms,AvgRatio,ratio@1,ratio@5,ratio@10,ratio@20,ratio@40,ratio@60,ratio@80,ratio@100" \
    > "$ds_root/dataset_summary.csv"

  PROFILE_COUNT="$(jq '.profiles | length' "$cfg")"
  ran_any=false

  for ((i=0;i<PROFILE_COUNT;i++)); do
    prof="$(jq -c ".profiles[$i]" "$cfg")"
    name="$(jq -r '.name' <<<"$prof")"
    [[ -n "$ONLY_PROFILE" && "$name" != "$ONLY_PROFILE" ]] && continue
    ran_any=true

    overrides="$(jq -c '.overrides // {}' <<<"$prof")"
    run_dir="$ds_root/$name"
    rm -rf "$run_dir"; mkdir -p "$run_dir/results"

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
    ' --argjson base "$BASE_JSON" --argjson ovr "$overrides" \
       --arg res "$run_dir/results" --arg gt "$gt")"

    echo "$final_cfg" > "$run_dir/config.json"

    java "${JVM_ARGS[@]}" -jar "$JAR" \
      "$run_dir/config.json" "$base" "$query" "$run_dir/keys.blob" \
      "$dim" "$run_dir" "$gt" "$BATCH_SIZE" \
      >"$run_dir/run.log" 2>&1 || exit 1

    metrics="$(extract_metrics "$run_dir/results")"
    IFS=',' read -r art ratio <<<"$metrics"
    rk="$(extract_ratio_per_k "$run_dir/results" "$KLIST")"

    echo "$name,$art,$ratio,$rk" >> "$ds_root/dataset_summary.csv"
    echo "$ds,$name,$art,$ratio,$rk" >> "$GLOBAL_SUMMARY"
  done

  [[ "$ran_any" == true ]] || die "No profile executed for $ds"
done
