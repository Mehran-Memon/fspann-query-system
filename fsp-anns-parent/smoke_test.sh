#!/usr/bin/env bash
set -Eeuo pipefail

# ============================================
# FSP-ANN FULL SMOKE TEST (Linux)
# ============================================
# End-to-end validation:
# 1. Verify config sanity (guards against known bad values)
# 2. Build merged config (base + profile)
# 3. Clean workspace
# 4. Run full system (index + query)
# 5. Analyze profiler CSV
#
# USE THIS BEFORE ANY FULL SWEEP OR PAPER RESULTS

echo "============================================"
echo "  FSP-ANN Smoke Test (Linux)"
echo "============================================"
echo ""

# ================= USER PARAMETERS =================

TEST_DATASET="SIFT1M"
TEST_PROFILE="M24"          # BASE or profile name
QUERY_LIMIT=200             # Smoke test queries only

# ================= PATHS =================

ROOT="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent"
JarPath="$ROOT/api/target/api-0.0.1-SNAPSHOT-shaded.jar"
OutRoot="/mnt/data/mehran/SMOKE_TEST"
Batch=100000

# ================= JVM SETTINGS =================

JvmArgs=(
  "-XX:+UseG1GC"
  "-XX:MaxGCPauseMillis=200"
  "-XX:+AlwaysPreTouch"
  "-Xmx8g"
  "-Dfile.encoding=UTF-8"
  "-Dreenc.mode=end"
)

# ================= HELPERS =================

die() { echo "ERROR: $*" >&2; exit 1; }
ensure_file() { [[ -f "$1" ]] || die "File not found: $1"; }

command -v java >/dev/null || die "java not found"
command -v jq   >/dev/null || die "jq not found"
ensure_file "$JarPath"

# ================= DATASET MAP =================

declare -A CFG BASE QUERY GT DIM

CFG["SIFT1M"]="$ROOT/config/src/main/resources/config_sift1m.json"
BASE["SIFT1M"]="/mnt/data/mehran/Datasets/SIFT1M/sift_base.fvecs"
QUERY["SIFT1M"]="/mnt/data/mehran/Datasets/SIFT1M/sift_query.fvecs"
GT["SIFT1M"]="/mnt/data/mehran/Datasets/SIFT1M/sift_query_groundtruth.ivecs"
DIM["SIFT1M"]=128

cfg="${CFG[$TEST_DATASET]}"
base="${BASE[$TEST_DATASET]}"
query="${QUERY[$TEST_DATASET]}"
gt="${GT[$TEST_DATASET]}"
dim="${DIM[$TEST_DATASET]}"

ensure_file "$cfg"
ensure_file "$base"
ensure_file "$query"
ensure_file "$gt"

echo "Dataset : $TEST_DATASET"
echo "Profile : $TEST_PROFILE"
echo "Queries : $QUERY_LIMIT"
echo "Dim     : $dim"
echo ""

# ================= STEP 1: CONFIG SANITY =================

echo "[STEP 1/5] Verifying configuration..."

maxCand=$(jq '.base.runtime.maxCandidateFactor' "$cfg")
gtSample=$(jq '.base.ratio.gtSample' "$cfg")
alpha=$(jq '.base.stabilization.alpha' "$cfg")

echo "  maxCandidateFactor = $maxCand"
echo "  gtSample           = $gtSample"
echo "  alpha              = $alpha"

if (( $(echo "$maxCand > 3" | bc -l) )); then
  echo "WARNING: maxCandidateFactor > 3 → ratio inflation risk"
  echo "         Recommended: 3"
  read -p "Press ENTER to continue anyway, Ctrl+C to abort..."
fi

if (( gtSample > 20 )); then
  echo "WARNING: gtSample too high → long hangs"
  echo "         Recommended: 10"
  read -p "Press ENTER to continue anyway, Ctrl+C to abort..."
fi

if (( $(echo "$alpha > 0.05" | bc -l) )); then
  echo "WARNING: alpha too aggressive → recall risk"
  echo "         Recommended: 0.02"
  read -p "Press ENTER to continue anyway, Ctrl+C to abort..."
fi

echo "  Config sanity OK"
echo ""

# ================= STEP 2: BUILD MERGED CONFIG =================

echo "[STEP 2/5] Building final config..."

CFG_JSON="$(cat "$cfg")"

if [[ "$TEST_PROFILE" == "BASE" ]]; then
  final_cfg="$(jq 'del(.profiles)' <<<"$CFG_JSON")"
else
  base_json="$(jq 'del(.profiles)' <<<"$CFG_JSON")"
  profile="$(jq ".profiles[] | select(.name == \"$TEST_PROFILE\")" <<<"$CFG_JSON")"
  [[ -n "$profile" ]] || die "Profile not found: $TEST_PROFILE"

  overrides="$(jq '.overrides' <<<"$profile")"

  final_cfg="$(jq -n '
    def deepmerge(a; b):
      reduce (b | keys[]) as $k
        (a;
         if (a[$k] | type) == "object" and (b[$k] | type) == "object"
         then .[$k] = deepmerge(a[$k]; b[$k])
         else .[$k] = b[$k]
         end);
    deepmerge($base; $ovr)
  ' --argjson base "$base_json" --argjson ovr "$overrides")"
fi

run_dir="$OutRoot/${TEST_DATASET}_${TEST_PROFILE}"
rm -rf "$run_dir"
mkdir -p "$run_dir/results"

final_cfg="$(jq '
  .output.resultsDir = $res |
  .ratio.source = "gt" |
  .ratio.gtPath = $gt |
  .ratio.gtSample = 10
' --arg res "$run_dir/results" --arg gt "$gt" <<<"$final_cfg")"

echo "$final_cfg" > "$run_dir/config.json"

echo "  Config written: $run_dir/config.json"
echo ""

# ================= STEP 3: RUN SYSTEM =================

echo "[STEP 3/5] Running smoke test..."
log="$run_dir/run.log"
start=$(date +%s)

java "${JvmArgs[@]}" \
  -Dcli.dataset="$TEST_DATASET" \
  -Dcli.profile="$TEST_PROFILE" \
  -Dquery.limit="$QUERY_LIMIT" \
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

end=$(date +%s)
elapsed=$((end - start))

echo "  Runtime: ${elapsed}s"
echo ""

# ================= STEP 4: ANALYZE RESULTS =================

echo "[STEP 4/5] Analyzing results..."

csv="$run_dir/results/profiler_metrics.csv"
ensure_file "$csv"

python3 <<EOF
import pandas as pd

df = pd.read_csv("$csv")

print(f"Queries: {len(df)}")
print("")
print("Ratio:")
print(f"  Mean   : {df['ratio'].mean():.3f}")
print(f"  Median : {df['ratio'].median():.3f}")
print(f"  Min    : {df['ratio'].min():.3f}")
print(f"  Max    : {df['ratio'].max():.3f}")
print("")
print("Precision:")
print(f"  Mean   : {df['precision'].mean():.3f}")
print("")
print("Latency (ms):")
print(f"  Server : {df['serverMs'].mean():.1f}")
print(f"  Client : {df['clientMs'].mean():.1f}")
print(f"  Total  : {(df['serverMs']+df['clientMs']).mean():.1f}")
print("")

ok_ratio = df['ratio'].mean() <= 1.30
ok_prec  = df['precision'].mean() >= 0.85

print("Status:")
print("  Ratio     :", "PASS" if ok_ratio else "FAIL")
print("  Precision :", "PASS" if ok_prec else "FAIL")

exit(0 if ok_ratio and ok_prec else 1)
EOF

# ================= STEP 5: GT CHECK =================

echo ""
echo "[STEP 5/5] GT Validation..."

if grep -q "GT VALIDATION PASSED" "$log"; then
  echo "✓ GT validation PASSED"
elif grep -q "GT VALIDATION FAILED" "$log"; then
  echo "✗ GT validation FAILED"
  grep -A 5 "GT VALIDATION FAILED" "$log"
  exit 1
else
  echo "? GT validation not found in log"
fi

echo ""
echo "============================================"
echo " Smoke test COMPLETE"
echo " Results ready for evaluation"
echo "============================================"
