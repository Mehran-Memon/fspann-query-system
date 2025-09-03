# ============================
# FSP-ANN batch runner (3 configs)
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
# A) Recall-first, B) Balanced (baseline), C) Throughput-first
$Configs = @(
    @{ Label="recall_first"; ProbePerTable=24; ProbeBitsMax=2; FanoutTarget=0.05 },
    @{ Label="balanced";     ProbePerTable=16; ProbeBitsMax=1; FanoutTarget=0.02 },
    @{ Label="throughput";   ProbePerTable=8;  ProbeBitsMax=1; FanoutTarget=0.01 }
)

# Datasets: only include ones you have base + query + groundtruth
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
    GT="E:\Research Work\Datasets\synthetic_data\synthetic_1024\groundtruth.ivecs" },

    # Cropped SIFT subsets
    @{ Name="SIFT_10M"; Dim=128;
    Base="E:\Research Work\Datasets\Sift1B\bigann_base.bvecs\bigann_10M.bvecs";
    Query="E:\Research Work\Datasets\Sift1B\bigann_query.bvecs\queries.bvecs";
    GT="E:\Research Work\Datasets\Sift1B\bigann_gnd\gnd\idx_10M.ivecs" },

    @{ Name="SIFT_50M"; Dim=128;
    Base="E:\Research Work\Datasets\Sift1B\bigann_base.bvecs\bigann_50M.bvecs";
    Query="E:\Research Work\Datasets\Sift1B\bigann_query.bvecs\queries.bvecs";
    GT="E:\Research Work\Datasets\Sift1B\bigann_gnd\gnd\idx_50M.ivecs" },

    @{ Name="SIFT_100M"; Dim=128;
    Base="E:\Research Work\Datasets\Sift1B\bigann_base.bvecs\bigann_100M.bvecs";
    Query="E:\Research Work\Datasets\Sift1B\bigann_query.bvecs\queries.bvecs";
    GT="E:\Research Work\Datasets\Sift1B\bigann_gnd\gnd\idx_100M.ivecs" }
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

# ---------- STEP 1: Generate keys if missing ----------
$ForceRegenKeys = $false  # set to $true to force a fresh keystore (e.g., after class changes)

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
$Combined = @()
$CombinedPath = Join-Path $OutRoot "all_datasets_topk_eval.csv"

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

        $jvm = @(
            "-Xms$Xms","-Xmx$XmxThis",
            "-XX:+UseG1GC","-XX:MaxGCPauseMillis=200",
            "-Dprobe.perTable=$($cfg.ProbePerTable)",
            "-Dprobe.bits.max=$($cfg.ProbeBitsMax)",
            "-Dfanout.target=$($cfg.FanoutTarget)",
            "-Deval.sweep=1-100",
            "-Daudit.enable=true","-Daudit.k=100","-Daudit.sample.every=200","-Daudit.worst.keep=50",
            "-Dexport.artifacts=true",
            "-Dfile.encoding=UTF-8",
            "-jar", (Resolve-Path $JarPath)
        )

        $app = @(
            (Resolve-Path $ConfigPath),
            $base, $query, (Resolve-Path $KeysPath),
            $dim,  (Resolve-Path $metaDir), $gt,
            "100000"  # sample cap / batch limit
        )

        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        & $Java @jvm @app *>&1 | Tee-Object -FilePath $logFile
        $sw.Stop()

        if ($LASTEXITCODE -ne 0) {
            Write-Warning "Run failed for $name ($label) — see $logFile"
            Pop-Location
            continue
        } else {
            Add-Content -Path $logFile -Value ("Elapsed: {0:N1} sec" -f ($sw.Elapsed.TotalSeconds))
        }

        # Collect the per-run top-k CSV (emitted under metaDir\results)
        $topkPath = Join-Path $metaDir "results\topk_evaluation.csv"
        if (Test-Path $topkPath) {
            $rows = Import-Csv $topkPath
            foreach ($r in $rows) {
                $r | Add-Member -NotePropertyName Dataset -NotePropertyValue $name
                $r | Add-Member -NotePropertyName Config  -NotePropertyValue $label
                $Combined += $r
            }
        } else {
            Write-Warning "No topk_evaluation.csv found for $name ($label)"
        }

        Pop-Location
    }
}

# ---------- WRITE COMBINED CSV ----------
if ($Combined.Count -gt 0) {
    $Combined | Export-Csv -NoTypeInformation -Path $CombinedPath
    Write-Host "Combined CSV saved to: $CombinedPath"
} else {
    Write-Warning "No rows collected; check per-dataset outputs."
}
