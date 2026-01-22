Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

############################################
# PATHS
############################################
$Jar = "F:\fspann-query-system\fsp-anns-parent\api\target\api-0.0.1-SNAPSHOT-shaded.jar"
$ConfigBase = "F:\fspann-query-system\fsp-anns-parent\config\src\main\resources\config.json"
$OutRoot = "G:\fsp-run"
$BatchSize = 100000

$JvmArgs = @(
    "-Xmx16g",
    "-XX:+UseG1GC",
    "-XX:MaxGCPauseMillis=200",
    "-XX:+AlwaysPreTouch",
    "-Ddisable.exit=true",
    "-Dreenc.mode=end",
    "-Dfile.encoding=UTF-8"
)

############################################
# DATASETS
############################################
$Datasets = @(
    @{ Name="SIFT1M"; Dim=128; Base="E:\Datasets\SIFT1M\sift_base.fvecs"; Query="E:\Datasets\SIFT1M\sift_query.fvecs"; GT="E:\Datasets\SIFT1M\sift_query_groundtruth.ivecs" },
    @{ Name="GLOVE100"; Dim=100; Base="E:\Datasets\glove-100\glove-100_base.fvecs"; Query="E:\Datasets\glove-100\glove-100_query.fvecs"; GT="E:\Datasets\glove-100\glove-100_groundtruth.ivecs" },
    @{ Name="REDCAPS"; Dim=512; Base="E:\Datasets\redcaps\redcaps_base.fvecs"; Query="E:\Datasets\redcaps\redcaps_query.fvecs"; GT="E:\Datasets\redcaps\redcaps_groundtruth.ivecs" }
)

############################################
# LOAD CONFIG
############################################
$Cfg = Get-Content $ConfigBase -Raw | ConvertFrom-Json
$Profiles = $Cfg.profiles
if ($Profiles.Count -eq 0) { throw "No profiles found" }

############################################
# HELPERS
############################################
function Clean-RunDir($d) {
    Remove-Item $d -Recurse -Force -ErrorAction SilentlyContinue
    New-Item -ItemType Directory -Path "$d\metadata","$d\points","$d\results" | Out-Null
}

function Extract-Summary($dir) {
    $f = Join-Path $dir "results\summary.csv"
    if (!(Test-Path $f)) { return @{ART="N/A";Ratio="N/A";Precision="N/A"} }
    $rows = Import-Csv $f
    return @{
        ART = $rows[-1].ART_ms
        Ratio = $rows[-1].AvgRatio
        Precision = $rows[-1].AvgPrecision
    }
}

############################################
# RUN
############################################
foreach ($dataset in $Datasets) {

    $dsName = $dataset.Name
    $base   = $dataset.Base
    $query  = $dataset.Query
    $gt     = $dataset.GT
    $dim    = $dataset.Dim

    if (-not (Test-Path $base)) {
        Write-Host "Skipping $dsName (missing base)"
        continue
    }

    if (-not (Test-Path $query)) {
        Write-Host "Skipping $dsName (missing query)"
        continue
    }

    if (-not (Test-Path $gt)) {
        Write-Host "Skipping $dsName (missing GT)"
        continue
    }

    Write-Host ""
    Write-Host "========================================"
    Write-Host "DATASET: $dsName (dim=$dim)"
    Write-Host "========================================"

    $datasetRoot = Join-Path $OutRoot $dsName
    New-Item -ItemType Directory -Force -Path $datasetRoot | Out-Null

    foreach ($profile in $cfgObj.profiles) {

        $label = $profile.name
        if ([string]::IsNullOrWhiteSpace($label)) { continue }

        $runDir = Join-Path $datasetRoot $label
        New-Item -ItemType Directory -Force -Path "$runDir\results" | Out-Null

        $finalConfig = Merge-ConfigObjects -Base $baseConfig -Overrides $profile.overrides

        if (-not $finalConfig.output) {
            $finalConfig | Add-Member NoteProperty output @{}
        }

        $finalConfig.output.resultsDir = "$runDir\results"

        if (-not $finalConfig.ratio) {
            $finalConfig | Add-Member NoteProperty ratio @{}
        }

        $finalConfig.ratio.source = "gt"
        $finalConfig.ratio.gtPath = $gt
        $finalConfig.ratio.autoComputeGT = $false
        $finalConfig.ratio.allowComputeIfMissing = $false

        $cfgFile = Join-Path $runDir "config.json"
        $finalConfig | ConvertTo-Json -Depth 64 | Set-Content $cfgFile

        $log = Join-Path $runDir "run.log"

        try {
            & java @JvmArgs `
                "-Dcli.dataset=$dsName" `
                "-Dcli.profile=$label" `
                -jar $JarPath `
                $cfgFile `
                $base `
                $query `
                "$runDir\keys.blob" `
                $dim `
                $runDir `
                $gt `
                $Batch `
                2>&1 | Tee-Object $log | Out-Null

            $status = "OK"
        }
        catch {
            $status = "FAIL"
        }

        Write-Host ("{0,-25} | {1}" -f $label, $status)
    }
}
