# ============================
# FSP-ANN batch runner (3 configs) — full indexing + querying
# Updated: precision-first + GT AUTO precompute each run
# ============================

# ---------- SETTINGS ----------
$JarPath    = "F:\fspann-query-system\fsp-anns-parent\api\target\api-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
$ConfigPath = "F:\fspann-query-system\fsp-anns-parent\config\src\main\resources\config.json"
$KeysPath   = "G:\fsp-run\metadata\keys.ser"   # will be created if missing
$MetaRoot   = "G:\fsp-run\metadata"            # per-dataset subfolders will be created
$OutRoot    = "G:\fsp-run\out"                 # where logs/CSVs will go

# Default JVM sizing (dataset-specific bump is applied later)
$Xms = "4g"
$Xmx = "16g"

# Java executable
$Java = "java"

# Clean behavior
$CleanPerRun = $true   # clean metadata/points/results for each dataset×config before running
$CleanAllNow = $false  # set $true to wipe ALL per-dataset metadata under $MetaRoot once and exit

function Get-XmxForDataset([string]$datasetName, [string]$defaultXmx) {
    if ($datasetName -in @("SIFT_50M","SIFT_100M","synthetic_1024")) { return "32g" }
    return $defaultXmx
}

# ----------------- EXPLORE/WIDTH CONFIGS (ordered) -----------------
$Configs = @(
# 💨 throughput
    @{ Label="throughput";
    ProbePerTable=6;  ProbeBitsMax=1; FanoutTarget=0.008;
    PaperM=10; PaperLambda=5; PaperDivisions=8; PaperMaxCandidates=80000 },

    # ⚖️ balanced
    @{ Label="balanced";
    ProbePerTable=12; ProbeBitsMax=1; FanoutTarget=0.012;
    PaperM=12; PaperLambda=6; PaperDivisions=10; PaperMaxCandidates=120000 },

    # 🎯 recall-first (kept label for continuity; system reports precision now)
    @{ Label="recall_first";
    ProbePerTable=28; ProbeBitsMax=3; FanoutTarget=0.035;
    PaperM=14; PaperLambda=8; PaperDivisions=12; PaperMaxCandidates=200000 }
)

# -------------- DATASETS (only include ones you have base+query) --------------
$Datasets = @(
    @{ Name="SIFT1M"; Dim=128;
    Base="E:\Research Work\Datasets\sift_dataset\sift_base.fvecs";
    Query="E:\Research Work\Datasets\sift_dataset\sift_query.fvecs" },

    @{ Name="Audio"; Dim=192;
    Base="E:\Research Work\Datasets\audio\audio_base.fvecs";
    Query="E:\Research Work\Datasets\audio\audio_query.fvecs" },

    @{ Name="GloVe100"; Dim=100;
    Base="E:\Research Work\Datasets\glove-100\glove-100_base.fvecs";
    Query="E:\Research Work\Datasets\glove-100\glove-100_query.fvecs" },

    @{ Name="Enron"; Dim=1369;
    Base="E:\Research Work\Datasets\Enron\enron_base.fvecs";
    Query="E:\Research Work\Datasets\Enron\enron_query.fvecs" },

    # Synthetic datasets
    @{ Name="synthetic_128"; Dim=128;
    Base="E:\Research Work\Datasets\synthetic_data\synthetic_128\base.fvecs";
    Query="E:\Research Work\Datasets\synthetic_data\synthetic_128\query.fvecs" },

    @{ Name="synthetic_256"; Dim=256;
    Base="E:\Research Work\Datasets\synthetic_data\synthetic_256\base.fvecs";
    Query="E:\Research Work\Datasets\synthetic_data\synthetic_256\query.fvecs" },

    @{ Name="synthetic_512"; Dim=512;
    Base="E:\Research Work\Datasets\synthetic_data\synthetic_512\base.fvecs";
    Query="E:\Research Work\Datasets\synthetic_data\synthetic_512\query.fvecs" },

    @{ Name="synthetic_1024"; Dim=1024;
    Base="E:\Research Work\Datasets\synthetic_data\synthetic_1024\base.fvecs";
    Query="E:\Research Work\Datasets\synthetic_data\synthetic_1024\query.fvecs" }
)

# ---------- PREP ROOT FOLDERS ----------
New-Item -ItemType Directory -Force -Path $OutRoot  | Out-Null
New-Item -ItemType Directory -Force -Path $MetaRoot | Out-Null

# ---------- HELPERS ----------
function Ensure-Files([string]$base,[string]$query) {
    $ok = $true
    if ([string]::IsNullOrWhiteSpace($base)  -or -not (Test-Path -LiteralPath $base))  { Write-Error "Missing base:  $base";  $ok = $false }
    if ([string]::IsNullOrWhiteSpace($query) -or -not (Test-Path -LiteralPath $query)) { Write-Error "Missing query: $query"; $ok = $false }
    return $ok
}

function Safe-Resolve([string]$Path, [bool]$AllowMissing = $false) {
    try {
        if ($AllowMissing) {
            if (-not (Test-Path -LiteralPath $Path)) { return $Path }
        }
        return (Resolve-Path -LiteralPath $Path).Path
    } catch {
        if ($AllowMissing) { return $Path } else { throw }
    }
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

function Clean-RunMetadata([string]$MetaDir,[string]$RunDir) {
    $paths = @(
        (Join-Path $MetaDir "metadata"),
        (Join-Path $MetaDir "points"),
        (Join-Path $RunDir  "results")
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

# ---------- STEP 2: RUN ALL DATASETS x CONFIGS ----------
$CombinedTopK      = @()
$CombinedPrecision = @()
$CombinedTopKPath      = Join-Path $OutRoot "all_datasets_topk_eval.csv"
$CombinedPrecisionPath = Join-Path $OutRoot "all_datasets_global_precision.csv"

foreach ($ds in $Datasets) {
    $name  = $ds.Name
    $dim   = [string]$ds.Dim
    $base  = $ds.Base
    $query = $ds.Query

    if (-not (Ensure-Files $base $query)) { continue }

    foreach ($cfg in $Configs) {
        $label   = $cfg.Label
        $metaDir = Join-Path $MetaRoot "$name\$label"
        $runDir  = Join-Path $OutRoot  "$name\$label"
        New-Item -ItemType Directory -Force -Path $metaDir | Out-Null
        New-Item -ItemType Directory -Force -Path $runDir  | Out-Null

        if ($CleanPerRun) {
            Write-Host "Cleaning metadata/points/results for $name ($label) at $metaDir ..."
            Clean-RunMetadata -MetaDir $metaDir -RunDir $runDir
        }

        Push-Location $runDir
        Get-ChildItem -Filter "*.csv" -ErrorAction SilentlyContinue | Remove-Item -Force -ErrorAction SilentlyContinue

        $XmxThis = Get-XmxForDataset $name $Xmx
        $logFile = Join-Path $runDir "run.log"

        # Dataset-specific widening for hard cases (e.g., 1024-dim)
        if ($name -eq "synthetic_1024" -and $label -eq "recall_first") {
            $cfg.PaperMaxCandidates = [Math]::Max($cfg.PaperMaxCandidates, 200000)
            $cfg.ProbePerTable = [Math]::Max($cfg.ProbePerTable, 32)
            $cfg.ProbeBitsMax  = [Math]::Max($cfg.ProbeBitsMax, 3)
            $cfg.FanoutTarget  = [Math]::Max($cfg.FanoutTarget, 0.04)
        }

        Write-Host "Running $name ($label) dim=$dim (Xmx=$XmxThis)"

        # ---- Per-config widening / target tuning for paper engine ----
        $passFlags = @()
        switch ($label) {
            "throughput" {
                $passFlags += "-Dpaper.target.mult=1.60"
                $passFlags += "-Dpaper.expand.radius.max=2"
                $passFlags += "-Dpaper.expand.radius.hard=4"
            }
            "balanced" {
                $passFlags += "-Dpaper.target.mult=1.75"
                $passFlags += "-Dpaper.expand.radius.max=3"
                $passFlags += "-Dpaper.expand.radius.hard=5"
            }
            "recall_first" {
                $passFlags += "-Dpaper.target.mult=1.85"
                $passFlags += "-Dpaper.expand.radius.max=3"
                $passFlags += "-Dpaper.expand.radius.hard=6"
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
            "-Dpaper.maxCandidates=$($cfg.PaperMaxCandidates)",
            "-Dpaper.buildThreshold=1000",
            "-Dcloak.noise=0.0",

            # write-through on so points hit Rocks/points immediately
            "-Dindex.writeThrough=true",

            # full scale (index + query)
            "-Dquery.only=false"
        ) + $passFlags + @(
        # evaluation wiring (precision on; GT will be AUTO)
            "-Deval.computePrecision=true",
            "-Deval.writeGlobalPrecisionCsv=true",
            "-Dbase.path=$base",
            "-Daudit.enable=false",
            "-Dexport.artifacts=true",
            "-Dfile.encoding=UTF-8",
            "-jar", (Resolve-Path $JarPath)
        )

        # Ensure keystore directory exists; don't Resolve-Path the file itself
        $KeysDir = Split-Path -Parent $KeysPath
        New-Item -ItemType Directory -Force -Path $KeysDir | Out-Null

        # ---- FORCE PRECOMPUTE EVERY RUN ----
        $gtArg = "AUTO"

        $app = @(
            (Safe-Resolve $ConfigPath),
            (Safe-Resolve $base),
            (Safe-Resolve $query),
            (Safe-Resolve $KeysPath $true),  # allow missing (will be created)
            $dim,
            (Safe-Resolve $metaDir),
            $gtArg,                          # AUTO => Java will precompute <query>.ivecs and use it
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
        $resultsDir = Join-Path $runDir "results"
        $topkPath   = Join-Path $resultsDir "topk_evaluation.csv"
        $gprec      = Join-Path $resultsDir "global_precision.csv"

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

        if (Test-Path $gprec) {
            try {
                $rows2 = Import-Csv $gprec
                foreach ($g in $rows2) {
                    $g | Add-Member -NotePropertyName Dataset -NotePropertyValue $name
                    $g | Add-Member -NotePropertyName Config  -NotePropertyValue $label
                    $CombinedPrecision += $g
                }
                Copy-Item $gprec (Join-Path $runDir "global_precision.csv") -Force
            } catch { Write-Warning "Failed to read $gprec : $_" }
        } else {
            Write-Warning "No global_precision.csv found for $name ($label)"
        }

        try {
            $summary = @()
            if (Test-Path $gprec) {
                $last = (Import-Csv $gprec) | Sort-Object {[int]$_.topK} | Select-Object -Last 1
                if ($null -ne $last) {
                    $summary += "GlobalPrecision@K$($last.topK): $([double]::Parse($last.global_precision)) (matches=$($last.matches)/retrieved=$($last.retrieved))"
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

if ($CombinedPrecision.Count -gt 0) {
    $CombinedPrecision | Export-Csv -NoTypeInformation -Path $CombinedPrecisionPath
    Write-Host "Combined Global Precision CSV saved to: $CombinedPrecisionPath"
} else {
    Write-Warning "No Global Precision rows collected; check per-dataset outputs."
}

Write-Host "All runs completed."