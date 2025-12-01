################################################################################
# FSP-ANN — MULTI-DATASET, MULTI-PROFILE RUNNER (WINDOWS POWERSHELL)
# Option A: Each dataset uses its own config_<dataset>.json from resources/
#
# Fully restored:
#   - Per-dataset merged CSVs
#   - Merged precision, results, topK, reencrypt, samples, worst, storage
#   - Merged metrics_summary.txt and storage_breakdown.txt
#
# Behavior is identical to your ideal system except:
#   - Loads config_sift1m.json, config_glove100.json, etc.
#   - Datasets run sequentially
################################################################################

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ==============================================================================
# PATHS (Modify if needed)
# ==============================================================================

$RepoRoot   = "F:\fspann-query-system\fsp-anns-parent"
$JarPath    = "$RepoRoot\api\target\api-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
$ConfigDir  = "$RepoRoot\config\src\main\resources"
$OutRoot    = "G:\fsp-run-new"

# Dataset configs (Option A)
$Config_SIFT1M  = "$ConfigDir\config_sift1m.json"
$Config_GLOVE   = "$ConfigDir\config_glove100.json"
$Config_REDCAPS = "$ConfigDir\config_redcaps.json"
$Config_DEEP1B  = "$ConfigDir\config_deep1b.json"
$Config_GIST1B  = "$ConfigDir\config_gist1b.json"

# ==============================================================================
# DATASET DEFINITIONS — FULL PATHS + DIMENSIONS
# ==============================================================================

$Datasets = @(
    @{
        Name = "RedCaps"
        Config = $Config_REDCAPS
        Base = "E:\Research Work\Datasets\redcaps\redcaps_base.fvecs"
        Query = "E:\Research Work\Datasets\redcaps\redcaps_query.fvecs"
        GT = "E:\Research Work\Datasets\redcaps\redcaps_groundtruth.ivecs"
        Dim = 512
    },
    @{
        Name = "SIFT1M"
        Config = $Config_SIFT1M
        Base = "E:\Datasets\SIFT1M\sift_base.fvecs"
        Query = "E:\Datasets\SIFT1M\sift_query.fvecs"
        GT = "E:\Datasets\SIFT1M\sift_groundtruth.ivecs"
        Dim = 128
    },
    @{
        Name = "glove-100"
        Config = $Config_GLOVE
        Base = "E:\Datasets\glove-100\glove_base.fvecs"
        Query = "E:\Datasets\glove-100\glove_query.fvecs"
        GT = "E:\Datasets\glove-100\glove_groundtruth.ivecs"
        Dim = 100
    },
    @{
        Name = "Deep1B"
        Config = $Config_DEEP1B
        Base = "E:\Datasets\Deep1B\deep1b_base.fvecs"
        Query = "E:\Datasets\Deep1B\deep1b_query.fvecs"
        GT = "E:\Datasets\Deep1B\deep1b_groundtruth.ivecs"
        Dim = 96
    },
    @{
        Name = "GIST1B"
        Config = $Config_GIST1B
        Base = "E:\Datasets\GIST1B\gist1b_base.fvecs"
        Query = "E:\Datasets\GIST1B\gist1b_query.fvecs"
        GT = "E:\Datasets\GIST1B\gist1b_groundtruth.ivecs"
        Dim = 960
    }
)

# ==============================================================================
# JVM SETTINGS
# ==============================================================================
$JvmArgs = @(
    "-XX:+UseG1GC","-XX:MaxGCPauseMillis=200","-XX:+AlwaysPreTouch",
    "-Ddisable.exit=true",
    "-Dfile.encoding=UTF-8",
    "-Dreenc.mode=end",
    "-Dreenc.minTouched=5000",
    "-Dreenc.batchSize=2000",
    "-Dlog.progress.everyN=0",
    "-Dpaper.buildThreshold=2000000"
)

$Batch = 100000


# ==============================================================================
# HELPERS
# ==============================================================================

function Combine-CSV {
    param([string[]]$Files, [string]$OutCsv)

    if (-not $Files -or $Files.Count -eq 0) { return }

    $dir = Split-Path -Parent $OutCsv
    New-Item -ItemType Directory -Force -Path $dir | Out-Null

    Remove-Item -LiteralPath $OutCsv -ErrorAction SilentlyContinue

    $headerWritten = $false

    foreach ($f in $Files) {
        if (-not (Test-Path $f)) { continue }

        $lines = Get-Content $f
        if ($lines.Count -lt 2) { continue }

        $profile = Split-Path (Split-Path $f -Parent) -Leaf  # .../<profile>/results/file.csv

        if (-not $headerWritten) {
            "profile,$($lines[0])" | Out-File $OutCsv
            $headerWritten = $true
        }

        foreach ($l in ($lines | Select-Object -Skip 1)) {
            if ($l.Trim().Length -gt 0) {
                "$profile,$l" | Add-Content $OutCsv
            }
        }
    }
}

# concat txt files
function Combine-Txt {
    param([string[]]$Files, [string]$OutTxt)

    if (-not $Files -or $Files.Count -eq 0) { return }

    $dir = Split-Path -Parent $OutTxt
    New-Item -ItemType Directory -Force -Path $dir | Out-Null

    Remove-Item -LiteralPath $OutTxt -ErrorAction SilentlyContinue

    foreach ($f in $Files) {
        $profile = Split-Path (Split-Path $f -Parent) -Leaf
        "===== PROFILE: $profile =====" | Add-Content $OutTxt
        Get-Content $f | Add-Content $OutTxt
        "" | Add-Content $OutTxt
    }
}

# ==============================================================================
# MAIN LOOP — DATASET → PROFILES → MERGE RESULTS
# ==============================================================================

foreach ($ds in $Datasets) {

    $Name   = $ds.Name
    $Conf   = $ds.Config
    $Base   = $ds.Base
    $Query  = $ds.Query
    $GT     = $ds.GT
    $Dim    = $ds.Dim

    Write-Host "===============================================================" -ForegroundColor Cyan
    Write-Host "RUNNING DATASET: $Name (Dim=$Dim)"
    Write-Host "Using config: $Conf"
    Write-Host "===============================================================" -ForegroundColor Cyan

    # Load profiles
    $cfg = (Get-Content $Conf -Raw) | ConvertFrom-Json
    $profiles = $cfg.profiles

    $datasetRoot = Join-Path $OutRoot $Name
    New-Item -ItemType Directory -Force -Path $datasetRoot | Out-Null

    foreach ($p in $profiles) {
        $label = $p.id
        $runDir = Join-Path $datasetRoot $label
        New-Item -ItemType Directory -Force -Path $runDir | Out-Null

        $keysFile = Join-Path $runDir "keystore.blob"
        $tmpConf  = Join-Path $runDir "config.json"

        # write profile-specific config
        ($cfg | ConvertTo-Json -Depth 100) | Out-File $tmpConf

        # Prepare java arg list
        $argList = @()
        $argList += $JvmArgs
        $argList += "-jar"
        $argList += $JarPath
        $argList += $tmpConf
        $argList += $Base
        $argList += $Query
        $argList += $keysFile
        $argList += "$Dim"
        $argList += $runDir
        $argList += $GT
        $argList += "$Batch"

        # run
        $log = Join-Path $runDir "run.log"
        & java @argList 2>&1 | Tee-Object $log

        Write-Host "Finished profile $label"
    }

    # ==============================================================================
    # MERGE RESULTS FOR THIS DATASET
    # ==============================================================================

    $results   = Get-ChildItem "$datasetRoot\*\results\results_table.csv" -ErrorAction SilentlyContinue
    $prec      = Get-ChildItem "$datasetRoot\*\results\global_precision.csv" -ErrorAction SilentlyContinue
    $topk      = Get-ChildItem "$datasetRoot\*\results\topk_evaluation.csv" -ErrorAction SilentlyContinue
    $reenc     = Get-ChildItem "$datasetRoot\*\results\reencrypt_metrics.csv" -ErrorAction SilentlyContinue
    $samples   = Get-ChildItem "$datasetRoot\*\results\retrieved_samples.csv" -ErrorAction SilentlyContinue
    $worst     = Get-ChildItem "$datasetRoot\*\results\retrieved_worst.csv" -ErrorAction SilentlyContinue
    $storSum   = Get-ChildItem "$datasetRoot\*\results\storage_summary.csv" -ErrorAction SilentlyContinue
    $metricsT  = Get-ChildItem "$datasetRoot\*\results\metrics_summary.txt" -ErrorAction SilentlyContinue
    $storBreak = Get-ChildItem "$datasetRoot\*\results\storage_breakdown.txt" -ErrorAction SilentlyContinue

    Combine-CSV ($results | Select-Object -Expand FullName) "$datasetRoot\combined_results.csv"
    Combine-CSV ($prec | Select-Object -Expand FullName) "$datasetRoot\combined_precision.csv"
    Combine-CSV ($topk | Select-Object -Expand FullName) "$datasetRoot\combined_topk.csv"
    Combine-CSV ($reenc | Select-Object -Expand FullName) "$datasetRoot\combined_reencrypt.csv"
    Combine-CSV ($samples | Select-Object -Expand FullName) "$datasetRoot\combined_samples.csv"
    Combine-CSV ($worst | Select-Object -Expand FullName) "$datasetRoot\combined_worst.csv"
    Combine-CSV ($storSum | Select-Object -Expand FullName) "$datasetRoot\combined_storage_summary.csv"

    Combine-Txt ($metricsT | Select-Object -Expand FullName) "$datasetRoot\combined_metrics_summary.txt"
    Combine-Txt ($storBreak | Select-Object -Expand FullName) "$datasetRoot\combined_storage_breakdown.txt"

    Write-Host "Merged results for $Name" -ForegroundColor Yellow
}

Write-Host "==============================================================="
Write-Host " ALL DATASETS COMPLETED SUCCESSFULLY "
Write-Host "==============================================================="
