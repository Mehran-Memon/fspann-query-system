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
foreach ($ds in $Datasets) {
    Write-Host ""
    Write-Host "========================================"
    Write-Host "DATASET: $($ds.Name)"
    Write-Host "========================================"

    $dsRoot = Join-Path $OutRoot $ds.Name
    New-Item -ItemType Directory -Force -Path $dsRoot | Out-Null

    foreach ($p in $Profiles) {
        $runDir = Join-Path $dsRoot $p.name
        Clean-RunDir $runDir

        $finalCfg = $Cfg.base | ConvertTo-Json -Depth 64 | ConvertFrom-Json
        foreach ($k in $p.overrides.PSObject.Properties.Name) {
            $finalCfg | Add-Member -Force -NotePropertyName $k -NotePropertyValue $p.overrides.$k
        }

        $finalCfg.output.resultsDir = "$runDir\results"
        $finalCfg.ratio.source = "gt"
        $finalCfg.ratio.gtPath = $ds.GT
        $finalCfg.ratio.autoComputeGT = $false
        $finalCfg.ratio.allowComputeIfMissing = $false
        $finalCfg.ratio.gtMismatchTolerance = 0.0

        $cfgFile = Join-Path $runDir "config.json"
        $finalCfg | ConvertTo-Json -Depth 64 | Set-Content $cfgFile

        & java @JvmArgs `
      "-Dcli.dataset=$($ds.Name)" `
      "-Dcli.profile=$($p.name)" `
      -jar $Jar `
      $cfgFile `
      $ds.Base
                $ds.Query
                (Join-Path $runDir "keys.blob") `
      $ds.Dim
                $runDir `
      $ds.GT
                $BatchSize `
      *> (Join-Path $runDir "run.log")

        $m = Extract-Summary $runDir
        Write-Host ("{0,-20} | ART={1,8} | Ratio={2,7} | Precision={3,7}" -f `
      $p.name, $m.ART, $m.Ratio, $m.Precision)
    }
}

Write-Host ""
Write-Host "ALL RUNS COMPLETE"
Write-Host "Results in $OutRoot"
