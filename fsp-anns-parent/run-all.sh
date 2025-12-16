#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

# =========================
# FSP-ANN PREMIUM RUNNER
# =========================
# Shows detailed metrics prominently: ART, Client, Server, Ratio
# Status simplified to ✓/✗, focus on metrics not status
# =========================

JarPath="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/api/target/api-0.0.1-SNAPSHOT-shaded.jar"
ConfigPath="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/config/src/main/resources/config_sift1m.json"
OutRoot="/mnt/data/mehran"
Batch=100000
MinDiskMB=$((1024 * 2))

# Colors for metrics
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m'

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

# ============ UTILITIES ============

die() {
  echo -e "${RED}✗ ERROR: $*${NC}" >&2
  exit 1
}

warn() {
  echo -e "${YELLOW}⚠ $*${NC}"
}

info() {
  echo -e "${BLUE}ℹ $*${NC}"
}

success() {
  echo -e "${GREEN}✓ $*${NC}"
}

ensure_file() {
  [[ -f "$1" ]] || die "File not found: $1"
}

# Color metric value based on quality
color_metric() {
  local metric="$1"
  local value="$2"
  local min="$3"
  local max="$4"

  if [[ "$value" == "N/A" ]]; then
    echo -e "${RED}$value${NC}"
    return
  fi

  # Remove "ms" or other suffixes for comparison
  local num=$(echo "$value" | sed 's/[^0-9.].*//')

  if (( $(echo "$num < $min" | bc -l 2>/dev/null || echo "0") )); then
    # Too low (good for latency, bad for ratio)
    if [[ "$max" -gt 10 ]]; then
      echo -e "${GREEN}$value${NC}"
    else
      echo -e "${YELLOW}$value${NC}"
    fi
  elif (( $(echo "$num > $max" | bc -l 2>/dev/null || echo "0") )); then
    # Too high (bad)
    echo -e "${RED}$value${NC}"
  else
    # In range (good)
    echo -e "${GREEN}$value${NC}"
  fi
}

# ============ VALIDATION ============

command -v java >/dev/null || die "java not found"
command -v jq   >/dev/null || die "jq not found"
ensure_file "$JarPath"
ensure_file "$ConfigPath"
mkdir -p "$OutRoot"

check_disk_space() {
  local available_mb=$(df "$OutRoot" 2>/dev/null | awk 'NR==2 {print $4}' || echo "0")
  if [[ "$available_mb" -lt "$MinDiskMB" ]]; then
    die "Insufficient disk space: need ${MinDiskMB}MB, have ${available_mb}MB"
  fi
  success "Disk space: ${available_mb}MB available"
}
check_disk_space

# ============ DATASETS ============

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

# ============ CONFIG LOADING ============

info "Loading config: $ConfigPath"
CFG_JSON="$(cat "$ConfigPath")" || die "Failed to read config"
BASE_JSON="$(jq -c 'del(.profiles)' <<<"$CFG_JSON" 2>/dev/null)" || die "Failed to parse config"
PROFILE_COUNT="$(jq '.profiles | length' <<<"$CFG_JSON" 2>/dev/null)" || die "Failed to parse profiles"
[[ "$PROFILE_COUNT" -gt 0 ]] || die "No profiles found"
success "Found $PROFILE_COUNT profiles"

# ============ METRICS EXTRACTION ============

extract_metrics() {
  local results_dir="$1"
  local art="N/A"
  local client_ms="N/A"
  local server_ms="N/A"
  local ratio="N/A"

  if [[ -f "$results_dir/metrics_summary.txt" ]]; then
    art=$(grep -oP 'ART\(ms\)=\K[0-9.]+' "$results_dir/metrics_summary.txt" 2>/dev/null || echo "N/A")
    ratio=$(grep -oP 'AvgRatio=\K[0-9.]+' "$results_dir/metrics_summary.txt" 2>/dev/null || echo "N/A")
  fi

  if [[ -f "$results_dir/profiler_metrics.csv" ]]; then
    client_ms=$(tail -100 "$results_dir/profiler_metrics.csv" 2>/dev/null | \
      awk -F',' 'NR>1 {sum+=$2; count++} END {if(count>0) printf "%.2f", sum/count; else print "N/A"}' 2>/dev/null || echo "N/A")
    server_ms=$(tail -100 "$results_dir/profiler_metrics.csv" 2>/dev/null | \
      awk -F',' 'NR>1 {sum+=$1; count++} END {if(count>0) printf "%.2f", sum/count; else print "N/A"}' 2>/dev/null || echo "N/A")
  fi

  # Format with units
  [[ "$art" != "N/A" ]] && art="${art}ms"
  [[ "$client_ms" != "N/A" ]] && client_ms="${client_ms}ms"
  [[ "$server_ms" != "N/A" ]] && server_ms="${server_ms}ms"

  echo "$art|$client_ms|$server_ms|$ratio"
}

# ============ DISPLAY METRICS ============

display_metric() {
  local name="$1"
  local value="$2"
  local target_min="$3"
  local target_max="$4"

  printf "  %-20s : " "$name"
  color_metric "$name" "$value" "$target_min" "$target_max"
}

show_error_details() {
  local log="$1"
  if [[ -f "$log" ]]; then
    echo -e "  ${RED}Last 10 lines of log:${NC}"
    tail -10 "$log" 2>/dev/null | sed 's/^/    /'
  fi
}

# ============ MAIN EXECUTION ============

total_runs=0
successful_runs=0
failed_runs=0
declare -A run_times
declare -A run_status
declare -A run_metrics

for idx in "${!DATASET_NAMES[@]}"; do
  ds="${DATASET_NAMES[$idx]}"
  dim="${DATASET_DIMS[$idx]}"
  base="${DATASET_BASE[$idx]}"
  query="${DATASET_QUERY[$idx]}"
  gt="${DATASET_GT[$idx]}"

  [[ -f "$base" ]] || { warn "Skipping $ds (missing base)"; continue; }
  [[ -f "$query" ]] || { warn "Skipping $ds (missing query)"; continue; }
  [[ -f "$gt" ]] || { warn "Skipping $ds (missing GT)"; continue; }

  echo ""
  echo -e "${CYAN} DATASET: $ds (dim=$dim) ${NC}"
  echo ""

  ds_root="$OutRoot/$ds"
  mkdir -p "$ds_root"

  # Table header
  echo -e "${MAGENTA}PROFILE${NC}                     │ ${MAGENTA}ART(ms)${NC}  │ ${MAGENTA}Client(ms)${NC} │ ${MAGENTA}Server(ms)${NC} │ ${MAGENTA}Ratio${NC}   │ ${MAGENTA}Status${NC} │ ${MAGENTA}Time${NC}"
  echo "───────────────────────────────┼─────────┼────────────┼────────────┼────────┼────────┼────────"

  for ((p=0; p<PROFILE_COUNT; p++)); do
    profile="$(jq -c ".profiles[$p]" <<<"$CFG_JSON" 2>/dev/null)" || continue
    label="$(jq -r '.name' <<<"$profile" 2>/dev/null)"
    [[ -n "$label" ]] || continue

    run_dir="$ds_root/$label"
    mkdir -p "$run_dir/results"

    overrides="$(jq -c '.overrides // {}' <<<"$profile" 2>/dev/null)"
    final_cfg="$(jq -n \
      --argjson base "$BASE_JSON" \
      --argjson ovr  "$overrides" \
      --arg resdir "$run_dir/results" \
      --arg gtpath "$gt" \
      '($base + $ovr) |
       (.output //= {}) | .output.resultsDir=$resdir |
       (.ratio //= {}) |
       .ratio.source="gt" |
       .ratio.gtPath=$gtpath |
       .ratio.autoComputeGT=false |
       .ratio.allowComputeIfMissing=false
      ' 2>/dev/null)" || {
        warn "Failed to generate config for $label"
        continue
      }

    echo "$final_cfg" > "$run_dir/config.json"
    log="$run_dir/run.log"

    ((total_runs++))
    start_time=$(date +%s)

    # Run Java
    if java "${JvmArgs[@]}" \
        "-Dcli.dataset=$ds" \
        "-Dcli.profile=$label" \
        "-Xloggc:$run_dir/gc.log" \
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
      status="${GREEN}✓${NC}"
      ((successful_runs++))
    else
      status="${RED}✗${NC}"
      ((failed_runs++))
    fi

    end_time=$(date +%s)
    elapsed=$((end_time - start_time))

    # Extract metrics
    metrics_line=$(extract_metrics "$run_dir/results")
    IFS='|' read -r art client_ms server_ms ratio <<<"$metrics_line"

    # Color the metrics
    art_colored=$(color_metric "ART" "$art" 0 100)
    client_colored=$(color_metric "Client" "$client_ms" 0 50)
    server_colored=$(color_metric "Server" "$server_ms" 0 50)
    ratio_colored=$(color_metric "Ratio" "$ratio" 1.0 1.3)

    # Verify results exist
    if [[ ! -f "$run_dir/results/summary.csv" ]]; then
      status="${YELLOW}⚠${NC}"
    fi

    # Format columns
    printf "%-32s│ %8s │ %10s │ %10s │ %6s │ %s │ %4ds\n" \
      "$label" "$art_colored" "$client_colored" "$server_colored" "$ratio_colored" "$status" "$elapsed"

    # Store for summary
    run_times["$ds:$label"]=$elapsed
    run_status["$ds:$label"]="$status"
    run_metrics["$ds:$label"]="$art|$client_ms|$server_ms|$ratio"
  done

  echo ""
done

# ============ DETAILED BREAKDOWN ============

for key in "${!run_metrics[@]}"; do
  metrics="${run_metrics[$key]}"
  IFS='|' read -r art client server ratio <<<"$metrics"

  echo -e "${MAGENTA}Run:${NC} $key"
  display_metric "ART (total)" "$art" 0 100
  display_metric "Client Time" "$client" 0 50
  display_metric "Server Time" "$server" 0 50
  display_metric "Candidate Ratio" "$ratio" 1.0 1.3
  echo ""
done

# ============ SUMMARY TABLE ============

echo ""
echo -e "${CYAN}                    EXECUTION SUMMARY                       ${NC}"
echo ""

printf "%-40s : %s\n" "Total Runs" "$total_runs"
printf "%-40s : ${GREEN}%s${NC}\n" "Successful" "$successful_runs"
printf "%-40s : ${RED}%s${NC}\n" "Failed" "$failed_runs"

if [[ "$total_runs" -gt 0 ]]; then
  success_pct=$((successful_runs * 100 / total_runs))
  if [[ "$success_pct" -ge 90 ]]; then
    color="${GREEN}"
  elif [[ "$success_pct" -ge 70 ]]; then
    color="${YELLOW}"
  else
    color="${RED}"
  fi
  printf "%-40s : ${color}%d%%${NC}\n" "Success Rate" "$success_pct"
fi

if [[ "$total_runs" -gt 0 ]]; then
  total_time=0
  for t in "${run_times[@]}"; do
    total_time=$((total_time + t))
  done
  avg_time=$((total_time / total_runs))
  printf "%-40s : %ds (avg: %ds)\n" "Total Execution Time" "$total_time" "$avg_time"
fi

echo ""

# ============ METRICS COMPARISON TABLE ============

echo -e "${CYAN}                   METRICS COMPARISON                ${NC}"
echo ""

printf "%-40s │ %-10s │ %-10s │ %-10s │ %-8s\n" \
  "RUN ID" "ART(ms)" "Client(ms)" "Server(ms)" "Ratio"
printf "%-40s-┼%-10s-┼%-10s-┼%-10s-┼%-8s\n" \
  "────────────────────────────────────────" "──────────" "──────────" "──────────" "────────"

for key in "${!run_metrics[@]}"; do
  metrics="${run_metrics[$key]}"
  IFS='|' read -r art client server ratio <<<"$metrics"

  # Remove 'ms' suffix for display
  art_num=$(echo "$art" | sed 's/ms//')
  client_num=$(echo "$client" | sed 's/ms//')
  server_num=$(echo "$server" | sed 's/ms//')

  # Color based on quality
  if [[ "$art_num" != "N/A" ]] && (( $(echo "$art_num < 100" | bc -l 2>/dev/null || echo "0") )); then
    art_colored="${GREEN}$art_num${NC}"
  else
    art_colored="${RED}$art_num${NC}"
  fi

  if [[ "$ratio" != "N/A" ]] && (( $(echo "$ratio < 1.3" | bc -l 2>/dev/null || echo "0") )); then
    ratio_colored="${GREEN}$ratio${NC}"
  else
    ratio_colored="${YELLOW}$ratio${NC}"
  fi

  printf "%-40s │ %10s │ %10s │ %10s │ %-8s\n" \
    "$key" "$art_colored" "$client_num" "$server_num" "$ratio_colored"
done

echo ""

# Find best and worst runs
best_art="999999"
best_key=""
worst_art="0"
worst_key=""

for key in "${!run_metrics[@]}"; do
  metrics="${run_metrics[$key]}"
  IFS='|' read -r art client server ratio <<<"$metrics"
  art_num=$(echo "$art" | sed 's/ms//')

  if [[ "$art_num" != "N/A" ]]; then
    if (( $(echo "$art_num < $best_art" | bc -l 2>/dev/null || echo "0") )); then
      best_art="$art_num"
      best_key="$key"
    fi
    if (( $(echo "$art_num > $worst_art" | bc -l 2>/dev/null || echo "0") )); then
      worst_art="$art_num"
      worst_key="$key"
    fi
  fi
done

if [[ -n "$best_key" ]]; then
  echo -e "${GREEN}✓ BEST PERFORMANCE:${NC} $best_key (ART: ${best_art}ms)"
fi

if [[ -n "$worst_key" ]] && [[ "$worst_key" != "$best_key" ]]; then
  echo -e "${YELLOW}⚠ NEEDS OPTIMIZATION:${NC} $worst_key (ART: ${worst_art}ms)"
fi

echo ""
echo -e "${GREEN}→ For paper, use best run: $best_key${NC}"
echo -e "${GREEN}→ All results in: $OutRoot${NC}"
echo ""

# ============ COMPLETION ============

echo -e "${CYAN}               EVALUATION COMPLETE                         ${NC}"
echo ""