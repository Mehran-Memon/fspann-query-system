# ============================
# FSP-ANN batch runner (3 configs) — measured
# ============================

# ---------- SETTINGS ----------
$JarPath    = "F:\fspann-query-system\fsp-anns-parent\api\target\api-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
$ConfigPath = "F:\fspann-query-system\fsp-anns-parent\config\src\main\resources\config.json"
$KeysPath   = "G:\fsp-run\metadata\keys.ser"
$MetaRoot   = "G:\fsp-run\metadata"   # per-dataset subfolders will be created
$OutRoot    = "G:\fsp-run\out"        # where logs/CSVs will go

# Default JVM sizing (dataset-specific bump is applied later)
$Xms = "4g"
$Xmx = "16g"

# Java executable
$Java = "java"

# ----------------- EXPLORE/WIDTH CONFIGS -----------------
# Goal: ratio in [1.0, 1.3] with decent latency
# ProbePerTable ↑ and FanoutTarget ↑ => more candidates => higher ratio, slower
# ProbePerTable ↓ and FanoutTarget ↓ => fewer candidates => lower ratio, faster
$Configs = @(
    @{ Label="recall_first"; ProbePerTable=24; ProbeBitsMax=2; FanoutTarget=0.06; TargetRatioMin=1.0; TargetRatioMax=1.3 },
    @{ Label="balanced";     ProbePerTable=16; ProbeBitsMax=1; FanoutTarget=0.03; TargetRatioMin=1.0; TargetRatioMax=1.3 },
    @{ Label="throughput";   ProbePerTable=10; ProbeBitsMax=1; FanoutTarget=0.015; TargetRatioMin=1.0; TargetRatioMax=1.3 }
)

# Datasets: include only those present (base + query + groundtruth)
$Datasets = @(
    @{ Name="SIFT1M"; Dim=128;
    Base="E:\Research Work\Datasets\sift_dataset\sift_base.fvecs";
    Query="E:\Research Work\Datasets\sift_dataset\sift_query.fvecs";
    GT="E:\Research Work\Datasets\sift_dataset\sift_groundtruth.ivecs" },

    @{ Name="Audio"; Dim=192;
    Base="E:\Research Work\Datasets\audio\audio_base.fvecs";
    Query="E:\Research Work\Datasets\audio\audio_query.fvecs";
    GT="E:\Research Work\Datasets\audio\audio_groundtruth.ivecs" },

    @{ Name="GloVe100"; Dim=100;
    Base="E:\Research Work\Datasets\glove-100\glove-100_base.fvecs";
    Query="E:\Research Work\Datasets\glove-100\glove-100_query.fvecs";
    GT="E:\Research Work\Datasets\glove-100\glove-100_groundtruth.ivecs" },

    @{ Name="Enron"; Dim=1369;
    Base="E:\Research Work\Datasets\Enron\enron_base.fvecs";
    Query="E:\Research Work\Datasets\Enron\enron_query.fvecs";
    GT="E:\Research Work\Datasets\Enron\enron_groundtruth.ivecs" },

    # Synthetic datasets
    @{ Name="synthetic_128"; Dim=128;
    Base="E:\Research Work\Datasets\synthetic_data\synthetic_128\base.fvecs";
    Query="E:\Research Work\Datasets\synthetic_data\synthetic_128\query.fvecs";
    GT="E:\Research Work\Datasets\synthetic_data\synthetic_128\groundtruth.ivecs" },

    @{ Name="synthetic_256"; Dim=256;
    Base="E:\Research Work\Datasets\synthetic_data\synthetic_256\base.fvecs";
    Query="E:\Research Work\Datasets\synthetic_data\synthetic_256\query.fvecs";
    GT="E:\Research Work\Datasets\synthetic_data\synthetic_256\groundtruth.ivecs" },

    @{ Name="synthetic_512"; Dim=512;
    Base="E:\Research Work\Datasets\synthetic_data\synthetic_512\base.fvecs";
    Query="E:\Research Work\Datasets\synthetic_data\synthetic_512\query.fvecs";
    GT="E:\Research Work\Datasets\synthetic_data\synthetic_512\groundtruth.ivecs" },

    @{ Name="synthetic_1024"; Dim=1024;
    Base="E:\Research Work\Datasets\synthetic_data\synthetic_1024\base.fvecs";
    Query="E:\Research Work\Datasets\synthetic_data\synthetic_1024\query.fvecs";
    GT="E:\Research Work\Datasets\synthetic_data\synthetic_1024\groundtruth.ivecs" }
)

# ---------- PREP FOLDERS ----------
New-Item -ItemType Directory -Force -Path $OutRoot  | Out-Null
New-Item -ItemType Directory -Force -Path $MetaRoot | Out-Null

# ---------- HELPERS ----------
function Get-XmxForDataset([string]$datasetName, [string]$defaultXmx) {
    if ($datasetName -in @("SIFT_50M","SIFT_100M")) { return "32g" }
    return $defaultXmx
}

function Ensure-Files([string]$base,[string]$query,[string]$gt) {
    $ok = $true
    if ([string]::IsNullOrWhiteSpace($base)  -or -not (Test-Path -LiteralPath $base))  { Write-Error "Missing base:  $base";  $ok = $false }
    if ([string]::IsNullOrWhiteSpace($query) -or -not (Test-Path -LiteralPath $query)) { Write-Error "Missing query: $query"; $ok = $false }
    if ([string]::IsNullOrWhiteSpace($gt)    -or -not (Test-Path -LiteralPath $gt))    { Write-Error "Missing GT:    $gt";    $ok = $false }
    return $ok
}

function Find-FirstValidDataset {
    foreach ($d in $Datasets) {
        if (Ensure-Files $d.Base $d.Query $d.GT) { return $d }
    }
    return $null
}

# Parse metrics exported by the app (created because -Dexport.artifacts=true)
function Read-RunMetrics([string]$metaDir) {
    $result = [ordered]@{ AvgRatio = $null; ARTms = $null }
    $msFile = Join-Path $metaDir "results\metrics_summary.txt"
    if (Test-Path $msFile) {
        $text = Get-Content $msFile -Raw
        if ($text -match 'ART\(ms\)=(?<art>[\d\.]+)') {
            $result.ARTms = [double]$Matches['art']
        }
        if ($text -match 'AvgRatio=(?<ratio>[\d\.]+)') {
            $result.AvgRatio = [double]$Matches['ratio']
        }
    }
    return $result
}

# ---------- STEP 1: Generate keys if missing ----------
$ForceRegenKeys = $true  # set to $true to force a fresh keystore (e.g., after class changes)

function New-Keys {
    $ds = Find-FirstValidDataset
    if ($null -eq $ds) { throw "No valid dataset found to generate keys. Check your dataset paths." }

    Write-Host "Generating keys.ser using dataset: $($ds.Name)"
    $metaDir = Join-Path $MetaRoot $ds.Name
    New-Item -ItemType Directory -Force -Path $metaDir | Out-Null

    $args = @(
        (Resolve-Path $ConfigPath),
        $ds.Base,
        $ds.Query,
        $KeysPath,
        [string]$ds.Dim,
        (Resolve-Path $metaDir),
        $ds.GT,
        "1"  # minimal batch for key generation
    )

    & $Java "-Xms$Xms" "-Xmx$Xmx" "-jar" (Resolve-Path $JarPath) @args
    if (-not (Test-Path -LiteralPath $KeysPath)) {
        throw "Failed to generate keys.ser"
    }
    Write-Host "keys.ser generated: $KeysPath"
}

$needKeys = -not (Test-Path -LiteralPath $KeysPath)
if (-not $needKeys) {
    $jarNewer = (Get-Item $JarPath).LastWriteTimeUtc -gt (Get-Item $KeysPath).LastWriteTimeUtc
    if ($ForceRegenKeys -or $jarNewer) {
        Write-Host "Removing incompatible/outdated keys.ser..."
        Remove-Item -LiteralPath $KeysPath -Force -ErrorAction SilentlyContinue
        $needKeys = $true
    }
}
if ($needKeys) { New-Keys }

# ---------- RUN ALL DATASETS x CONFIGS ----------
$CombinedTopK = @()
$CombinedTopKPath   = Join-Path $OutRoot "all_datasets_topk_eval.csv"
$CombinedSummary    = @()
$CombinedSummaryPath = Join-Path $OutRoot "CombinedSummary.csv"

foreach ($ds in $Datasets) {
    $name  = $ds.Name
    $dim   = [string]$ds.Dim
    $base  = $ds.Base
    $query = $ds.Query
    $gt    = $ds.GT

    if (-not (Ensure-Files $base $query $gt)) { continue }

    foreach ($cfg in $Configs) {
        $label   = $cfg.Label
        $metaDir = Join-Path $MetaRoot "$name\$label"
        $runDir  = Join-Path $OutRoot  "$name\$label"
        New-Item -ItemType Directory -Force -Path $metaDir | Out-Null
        New-Item -ItemType Directory -Force -Path $runDir  | Out-Null

        Push-Location $runDir
        # Clean old CSVs to avoid mixing across runs
        Get-ChildItem -Filter "*.csv" -ErrorAction SilentlyContinue | Remove-Item -Force -ErrorAction SilentlyContinue

        $XmxThis = Get-XmxForDataset $name $Xmx
        $logFile = Join-Path $runDir "run.log"

        Write-Host "=== Running $name ($label) dim=$dim (Xmx=$XmxThis) ==="

        # JVM & system knobs (no code changes)
        $jvm = @(
            "-Xms$Xms","-Xmx$XmxThis",
            "-XX:+UseG1GC","-XX:MaxGCPauseMillis=200","-XX:+AlwaysPreTouch",
            "-Dfile.encoding=UTF-8",
            "-Ddisable.exit=true",                 # let runner regain control
            "-Dexport.artifacts=true",             # export metrics CSVs
            "-Daudit.enable=true","-Daudit.k=100","-Daudit.sample.every=200","-Daudit.worst.keep=50",
            "-Dprobe.perTable=$($cfg.ProbePerTable)",
            "-Dprobe.bits.max=$($cfg.ProbeBitsMax)",
            "-Dfanout.target=$($cfg.FanoutTarget)",
            "-Deval.sweep=1-100",
            "-jar", (Resolve-Path $JarPath)
        )

        $app = @(
            (Resolve-Path $ConfigPath),
            $base, $query, (Resolve-Path $KeysPath),
            $dim,  (Resolve-Path $metaDir), $gt,
            "100000"  # sample cap / batch limit (safe default)
        )

        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        & $Java @jvm @app *>&1 | Tee-Object -FilePath $logFile
        $sw.Stop()

        if ($LASTEXITCODE -ne 0) {
            Write-Warning "Run failed for $name ($label) -- see $logFile"
            Pop-Location
            continue
        } else {
            Add-Content -Path $logFile -Value ("Elapsed: {0:N1} sec" -f ($sw.Elapsed.TotalSeconds))
        }

        # ---- Collect the per-run Top-K CSV (emitted under metaDir\results) ----
        $topkPath = Join-Path $metaDir "results\topk_evaluation.csv"
        if (Test-Path $topkPath) {
            $rows = Import-Csv $topkPath
            foreach ($r in $rows) {
                $r | Add-Member -NotePropertyName Dataset -NotePropertyValue $name
                $r | Add-Member -NotePropertyName Config  -NotePropertyValue $label
                $CombinedTopK += $r
            }
        } else {
            Write-Warning "No topk_evaluation.csv found for $name ($label)"
        }

        # ---- Parse metrics_summary for AvgRatio & ART(ms) ----
        $metrics = Read-RunMetrics $metaDir
        $avgRatio = $metrics.AvgRatio
        $artMs    = $metrics.ARTms
        $elapsed  = [Math]::Round($sw.Elapsed.TotalSeconds,1)

        # Classify vs. target window
        $status = "unknown"
        if ($avgRatio -ne $null) {
            if ($avgRatio -ge $cfg.TargetRatioMin -and $avgRatio -le $cfg.TargetRatioMax) {
                $status = "OK"
            } elseif ($avgRatio -lt $cfg.TargetRatioMin) {
                $status = "LOW_RATIO"     # consider raising ProbePerTable or FanoutTarget
            } else {
                $status = "HIGH_RATIO"    # consider lowering ProbePerTable or FanoutTarget
            }
        }

        # Record summary row
        $CombinedSummary += [pscustomobject]@{
            Dataset       = $name
            Config        = $label
            Dim           = $dim
            AvgRatio      = if ($avgRatio -ne $null) { "{0:N4}" -f $avgRatio } else { "" }
            ARTms         = if ($artMs   -ne $null) { "{0:N3}" -f $artMs } else { "" }
            ElapsedSec    = $elapsed
            ProbePerTable = $cfg.ProbePerTable
            ProbeBitsMax  = $cfg.ProbeBitsMax
            FanoutTarget  = $cfg.FanoutTarget
            Status        = $status
        }

        Pop-Location
    }
}

# ---------- WRITE COMBINED CSVs ----------
if ($CombinedTopK.Count -gt 0) {
    $CombinedTopK | Export-Csv -NoTypeInformation -Path $CombinedTopKPath
    Write-Host "Top-K combined CSV saved to: $CombinedTopKPath"
} else {
    Write-Warning "No Top-K rows collected; check per-dataset outputs."
}

if ($CombinedSummary.Count -gt 0) {
    $CombinedSummary |
            Sort-Object Dataset, Config |
            Export-Csv -NoTypeInformation -Path $CombinedSummaryPath
    Write-Host "Run summary saved to: $CombinedSummaryPath"
} else {
    Write-Warning "No summary rows collected; check runs/logs."
}
