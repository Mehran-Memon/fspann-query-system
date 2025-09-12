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

# Clean behavior
$CleanPerRun = $true   # clean metadata/points/results for each dataset×config before running
$CleanAllNow = $false  # set $true to wipe ALL per-dataset metadata under $MetaRoot once and exit

# Force key (re)generation when jar changed / class schema changed
$ForceRegenKeys = $false

# ----------------- EXPLORE/WIDTH CONFIGS (ordered) -----------------
$Configs = @(
# 💨 throughput: smallest fanout, fewer probes, coarse partitions → ratio ~1.3–1.4, best ART
    @{ Label="throughput";
    ProbePerTable=6;  ProbeBitsMax=1; FanoutTarget=0.008;
    PaperM=10; PaperLambda=5; PaperDivisions=6; PaperMaxCandidates=12000 },

    # ⚖️ balanced: mid fanout/probes, moderate partitions → ratio ~1.2–1.35, good ART
    @{ Label="balanced";
    ProbePerTable=12; ProbeBitsMax=1; FanoutTarget=0.012;
    PaperM=12; PaperLambda=6; PaperDivisions=8; PaperMaxCandidates=20000 },

    # 🎯 recall-first: wider fanout/probes, finer partitions → ratio ~1.0–1.15, higher ART
    @{ Label="recall_first";
    ProbePerTable=20; ProbeBitsMax=2; FanoutTarget=0.025;
    PaperM=14; PaperLambda=7; PaperDivisions=10; PaperMaxCandidates=50000 }
)

# -------------- DATASETS (only include ones you have base+query+GT) --------------
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

# ---------- PREP ROOT FOLDERS ----------
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

# ---------- FAST DELETE ----------
function Invoke-FastDelete([string]$PathToDelete) {
    if (-not (Test-Path -LiteralPath $PathToDelete)) { return }
    $item = Get-Item -LiteralPath $PathToDelete -ErrorAction SilentlyContinue
    if ($null -eq $item) { return }

    if (-not $item.PSIsContainer) {
        Remove-Item -LiteralPath $PathToDelete -Force -ErrorAction SilentlyContinue
        return
    }

    $empty = New-Item -ItemType Directory -Path (Join-Path $env:TEMP ("empty_" + [guid]::NewGuid())) -Force
    try {
        robocopy $empty.FullName $PathToDelete /MIR /NFL /NDL /NJH /NJS /NC /NS /NP | Out-Null
    } finally {
        try { Remove-Item -LiteralPath $PathToDelete -Recurse -Force -ErrorAction SilentlyContinue } catch {}
        try { Remove-Item -LiteralPath $empty.FullName -Recurse -Force -ErrorAction SilentlyContinue } catch {}
    }
}

function Clean-RunMetadata([string]$MetaDir) {
    $paths = @(
        (Join-Path $MetaDir "metadata"),
        (Join-Path $MetaDir "points"),
        (Join-Path $MetaDir "results")
    )
    foreach ($p in $paths) {
        if (Test-Path -LiteralPath $p) {
            Write-Host "Cleaning $p ..."
            Invoke-FastDelete $p
        }
        New-Item -ItemType Directory -Force -Path $p | Out-Null
    }
}

function Clean-AllMetadataUnderRoot([string]$MetaRootPath, [string]$KeysFullPath) {
    if (-not (Test-Path -LiteralPath $MetaRootPath)) { return }
    $keysLeaf = Split-Path -Leaf $KeysFullPath
    $children = Get-ChildItem -LiteralPath $MetaRootPath -Force -ErrorAction SilentlyContinue
    foreach ($c in $children) {
        if (-not $c.PSIsContainer -and $c.Name -ieq $keysLeaf) { continue }
        Write-Host "Wiping $($c.FullName) ..."
        Invoke-FastDelete $c.FullName
    }
}

# ---------- STEP 1: Optional global clean & exit ----------
if ($CleanAllNow) {
    Write-Host "Global clean requested. Removing all dataset/config metadata under $MetaRoot (keeping keys at $KeysPath)."
    Clean-AllMetadataUnderRoot -MetaRootPath $MetaRoot -KeysFullPath $KeysPath
    Write-Host "Global clean completed."
    return
}

# ---------- STEP 2: Generate keys if missing ----------
function New-Keys {
    $ds = Find-FirstValidDataset
    if ($null -eq $ds) { throw "No valid dataset found to generate keys. Check your dataset paths." }

    Write-Host "Generating keys.ser (query-only) using dataset: $($ds.Name)"
    $metaDir = Join-Path $MetaRoot $ds.Name
    New-Item -ItemType Directory -Force -Path $metaDir | Out-Null

    $jvm = @(
        "-Xms$Xms","-Xmx$Xmx",
        "-Dquery.only=true",
        "-Ddisable.exit=true",
        "-jar", (Resolve-Path $JarPath)
    )

    $args = @(
        (Resolve-Path $ConfigPath),
        "POINTS_ONLY",
        $ds.Query,
        $KeysPath,
        [string]$ds.Dim,
        (Resolve-Path $metaDir),
        $ds.GT,
        "100000"
    )

    & $Java @jvm @args
    if (-not (Test-Path -LiteralPath $KeysPath)) {
        throw "Failed to generate keys.ser"
    }
    Write-Host "keys.ser generated: $KeysPath"
}

# If keys missing or forced
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

# ---------- STEP 3: RUN ALL DATASETS x CONFIGS ----------
$CombinedTopK   = @()
$CombinedRecall = @()
$CombinedTopKPath    = Join-Path $OutRoot "all_datasets_topk_eval.csv"
$CombinedRecallPath  = Join-Path $OutRoot "all_datasets_global_recall.csv"

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

        if ($CleanPerRun) {
            Write-Host "Cleaning metadata/points/results for $name ($label) at $metaDir ..."
            Clean-RunMetadata -MetaDir $metaDir
        }

        Push-Location $runDir
        Get-ChildItem -Filter "*.csv" -ErrorAction SilentlyContinue | Remove-Item -Force -ErrorAction SilentlyContinue

        $XmxThis = Get-XmxForDataset $name $Xmx
        $logFile = Join-Path $runDir "run.log"

        Write-Host "=== Running $name ($label) dim=$dim (Xmx=$XmxThis) ==="

        # ---- Per-config widening / target tuning for paper engine ----
        $passFlags = @()
        switch ($label) {
            "throughput" {
                $passFlags += "-Dpaper.target.mult=1.55"
                $passFlags += "-Dpaper.expand.radius.max=2"
                $passFlags += "-Dpaper.expand.radius.hard=3"
            }
            "balanced" {
                $passFlags += "-Dpaper.target.mult=1.60"
                $passFlags += "-Dpaper.expand.radius.max=2"
                $passFlags += "-Dpaper.expand.radius.hard=4"
            }
            "recall_first" {
                $passFlags += "-Dpaper.target.mult=1.70"
                $passFlags += "-Dpaper.expand.radius.max=2"
                $passFlags += "-Dpaper.expand.radius.hard=5"
            }
        }

        # JVM/system tuning
        $jvm = @(
            "-Xms$Xms","-Xmx$XmxThis",
            "-XX:+UseG1GC","-XX:MaxGCPauseMillis=200","-XX:+AlwaysPreTouch",
            "-Ddisable.exit=true",

            # multiprobe knobs (harmless in paper mode, kept for A/B):
            "-Dprobe.perTable=$($cfg.ProbePerTable)",
            "-Dprobe.bits.max=$($cfg.ProbeBitsMax)",
            "-Dfanout.target=$($cfg.FanoutTarget)",

            # --- force partitioned + paper engine with per-config params ---
            "-Dfspann.mode=partitioned",
            "-Dpaper.mode=true",
            "-Dpaper.m=$($cfg.PaperM)",
            "-Dpaper.lambda=$($cfg.PaperLambda)",
            "-Dpaper.divisions=$($cfg.PaperDivisions)",
            "-Dpaper.maxCandidates=$($cfg.PaperMaxCandidates)"
        ) + $passFlags + @(
        # evaluation wiring
            "-Deval.computePrecision=false",
            "-Dbase.path=$base",
            "-Daudit.enable=false",
            "-Dexport.artifacts=true",
            "-Dfile.encoding=UTF-8",
            "-jar", (Resolve-Path $JarPath)
        )

        $app = @(
            (Resolve-Path $ConfigPath),
            $base, $query, (Resolve-Path $KeysPath),
            $dim,  (Resolve-Path $metaDir), $gt,
            "100000"
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

        # Collect per-run CSVs
        $resultsDir = Join-Path $metaDir "results"
        $topkPath   = Join-Path $resultsDir "topk_evaluation.csv"
        $grecall    = Join-Path $resultsDir "global_recall.csv"

        if (Test-Path $topkPath) {
            try {
                $rows = Import-Csv $topkPath
                foreach ($r in $rows) {
                    $r | Add-Member -NotePropertyName Dataset -NotePropertyValue $name
                    $r | Add-Member -NotePropertyName Config  -NotePropertyValue $label
                    $CombinedTopK += $r
                }
                Copy-Item $topkPath (Join-Path $runDir "topk_evaluation.csv") -Force
            } catch { Write-Warning "Failed to read $topkPath : $_" }
        } else {
            Write-Warning "No topk_evaluation.csv found for $name ($label)"
        }

        if (Test-Path $grecall) {
            try {
                $rows2 = Import-Csv $grecall
                foreach ($g in $rows2) {
                    $g | Add-Member -NotePropertyName Dataset -NotePropertyValue $name
                    $g | Add-Member -NotePropertyName Config  -NotePropertyValue $label
                    $CombinedRecall += $g
                }
                Copy-Item $grecall (Join-Path $runDir "global_recall.csv") -Force
            } catch { Write-Warning "Failed to read $grecall : $_" }
        } else {
            Write-Warning "No global_recall.csv found for $name ($label)"
        }

        try {
            $summary = @()
            if (Test-Path $grecall) {
                $last = (Import-Csv $grecall) | Sort-Object {[int]$_.topK} | Select-Object -Last 1
                if ($null -ne $last) {
                    $summary += "GlobalRecall@K$($last.topK): $([double]::Parse($last.global_recall)) (matches=$($last.matches)/truth=$($last.truth))"
                }
            }
            $logTail = (Get-Content $logFile | Select-String -Pattern "Average Client Query Time", "Average Ratio") -join "`n"
            Add-Content -Path (Join-Path $runDir "summary.txt") -Value @"
Dataset : $name
Config  : $label
Dim     : $dim
Elapsed : {0:N1} sec
$($summary -join "`n")
            $logTail
"@ -f ($sw.Elapsed.TotalSeconds)
        } catch {}

        Pop-Location
    }
}

# ---------- WRITE COMBINED CSVs ----------
if ($CombinedTopK.Count -gt 0) {
    $CombinedTopK | Export-Csv -NoTypeInformation -Path $CombinedTopKPath
    Write-Host "Combined TopK CSV saved to: $CombinedTopKPath"
} else {
    Write-Warning "No TopK rows collected; check per-dataset outputs."
}

if ($CombinedRecall.Count -gt 0) {
    $CombinedRecall | Export-Csv -NoTypeInformation -Path $CombinedRecallPath
    Write-Host "Combined Global Recall CSV saved to: $CombinedRecallPath"
} else {
    Write-Warning "No Global Recall rows collected; check per-dataset outputs."
}

Write-Host "All runs completed."