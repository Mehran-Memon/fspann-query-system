# ---------- SETTINGS ----------
$JarPath = "F:\fspann-query-system\fsp-anns-parent\api\target\api-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
$ConfigPath   = "F:\fspann-query-system\fsp-anns-parent\config\src\main\resources\config.json"
$KeysPath     = "G:\fsp-run\metadata\keys.ser"
$MetaRoot     = "G:\fsp-run\metadata"          # per-dataset subfolders will be created
$OutRoot      = "G:\fsp-run\out"                # where logs/CSVs will go

# Shared JVM flags (SAME CONFIGS across datasets for fair comparison)
$ProbePerTable = 16
$ProbeBitsMax  = 1
$FanoutTarget  = 0.02
$EvalSweep     = "1-100"
$Xms           = "4g"
$Xmx           = "16g"

# Java executable
$Java = "java"

# Datasets: only include ones you have base + query + groundtruth
$Datasets = @(
    @{ Name="SIFT1M";  Dim=128;
       Base="E:\Research Work\Datasets\sift_dataset\sift_base.fvecs";
       Query="E:\Research Work\Datasets\sift_dataset\sift_query.fvecs";
       GT="E:\Research Work\Datasets\sift_dataset\sift_groundtruth.ivecs" },

    @{ Name="Audio";   Dim=192;
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
       GT="E:\Research Work\Datasets\Enron\enron_groundtruth.ivecs" }

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

    # You can add SIFT1B later; running it here will be very long on this box.
    # @{ Name="SIFT1B"; Dim=128;
    #    Base="E:\Research Work\Datasets\Sift1B\bigann_base.bvecs\1milliard.p1.siftbin";
    #    Query="E:\Research Work\Datasets\Sift1B\bigann_query.bvecs\queries.bvecs";
    #    GT="E:\Research Work\Datasets\Sift1B\bigann_gnd\gnd\idx_1000M.ivecs" }
)

# Ensure directories
New-Item -ItemType Directory -Force -Path $OutRoot | Out-Null
New-Item -ItemType Directory -Force -Path $MetaRoot | Out-Null

# ---------- STEP 1: Generate keys if missing ----------
if (-not (Test-Path $KeysPath)) {
    Write-Host "keys.ser not found. Generating using first dataset..."
    $ds = $Datasets[0]  # use first dataset just to generate keys
    $metaDir = Join-Path $MetaRoot $ds.Name
    New-Item -ItemType Directory -Force -Path $metaDir | Out-Null

    # Use dummy metadata path and groundtruth to satisfy Java argument parser
    $dummyMeta = $metaDir
    $dummyGT   = $ds.GT
    $dummyDim  = $ds.Dim
    $dummyBatch = 1  # minimal batch for key generation

    $args = @(
        (Resolve-Path $ConfigPath),
        $ds.Base,
        $ds.Query,
        $KeysPath,
        $dummyDim,
        $dummyMeta,
        $dummyGT,
        $dummyBatch
    )

    & $Java "-Xms$Xms" "-Xmx$Xmx" "-jar" (Resolve-Path $JarPath) @args

    if (-not (Test-Path $KeysPath)) {
        Write-Error "Failed to generate keys.ser. Cannot continue."
        exit
    }
    Write-Host "keys.ser generated successfully."
}


$Combined = @()
$CombinedPath = Join-Path $OutRoot "all_datasets_topk_eval.csv"

foreach ($ds in $Datasets) {
    $name   = $ds.Name
    $dim    = [string]$ds.Dim
    $base   = $ds.Base
    $query  = $ds.Query
    $gt     = $ds.GT

    $metaDir = Join-Path $MetaRoot  $name
    $runDir  = Join-Path $OutRoot   $name
    New-Item -ItemType Directory -Force -Path $metaDir | Out-Null
    New-Item -ItemType Directory -Force -Path $runDir  | Out-Null

    Push-Location $runDir

    # Clean old CSVs to avoid mixing
    Get-ChildItem -Filter "*.csv" -ErrorAction SilentlyContinue | Remove-Item -Force -ErrorAction SilentlyContinue

    Write-Host "=== Running $name (dim=$dim) ==="

    # Build the argument list cleanly (no --% needed)
    $jvm = @(
        "-Xms$Xms","-Xmx$Xmx",
        "-Dprobe.perTable=$ProbePerTable",
        "-Dprobe.bits.max=$ProbeBitsMax",
        "-Dfanout.target=$FanoutTarget",
        "-Deval.sweep=$EvalSweep",
        "-Daudit.enable=true","-Daudit.k=100","-Daudit.sample.every=200","-Daudit.worst.keep=50",
        "-Dexport.artifacts=true",
        "-Dfile.encoding=UTF-8",
        "-jar", (Resolve-Path $JarPath)
    )

    $app = @(
        (Resolve-Path $ConfigPath),
        $base, $query, (Resolve-Path $KeysPath),
        $dim,  (Resolve-Path $metaDir), $gt,
        "100000"  # sample cap / batch limit as in your previous runs
    )

    & $Java @jvm @app
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "Run failed for $name"
        Pop-Location
        continue
    }

    # Collect the per-run top-k CSV (assuming the jar emits 'topk_evaluation.csv')
    $topkPath = Join-Path $metaDir "results\topk_evaluation.csv"
    if (Test-Path $topkPath) {
        $rows = Import-Csv $topkPath
        foreach ($r in $rows) {
            # add dataset label
            $r | Add-Member -NotePropertyName Dataset -NotePropertyValue $name
            $Combined += $r
        }
    } else {
        Write-Warning "No topk_evaluation.csv found for $name"
    }

    Pop-Location
}

# Write the combined CSV
if ($Combined.Count -gt 0) {
    $Combined | Export-Csv -NoTypeInformation -Path $CombinedPath
    Write-Host "Combined CSV saved to: $CombinedPath"
} else {
    Write-Warning "No rows collected; check per-dataset outputs."
}