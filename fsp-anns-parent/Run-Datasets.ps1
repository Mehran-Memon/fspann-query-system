Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ---- JAR RESOLUTION (supports thin + shaded) ----
$ApiDir = "F:\fspann-query-system\fsp-anns-parent\api\target"

$ShadedJar = Join-Path $ApiDir "api-0.0.1-SNAPSHOT-shaded.jar"
$ThinJar   = Join-Path $ApiDir "api-0.0.1-SNAPSHOT.jar"

if (Test-Path $ShadedJar) {
    $JarPath = $ShadedJar
    Write-Host "Using SHADED JAR: $JarPath" -ForegroundColor Green
}
elseif (Test-Path $ThinJar) {
    $JarPath = $ThinJar
    Write-Host "Using THIN JAR: $JarPath" -ForegroundColor Yellow
}
else {
    throw "No runnable API JAR found in $ApiDir"
}

$Configs = @(
    @{ Name="SIFT1M";   Path="F:\fspann-query-system\fsp-anns-parent\config\src\main\resources\config_sift1m.json" },
    @{ Name="GLOVE100"; Path="F:\fspann-query-system\fsp-anns-parent\config\src\main\resources\config_glove100.json" },
    @{ Name="REDCAPS";  Path="F:\fspann-query-system\fsp-anns-parent\config\src\main\resources\config_redcaps.json" }
)

$JvmArgs = @(
    "-XX:+UseG1GC","-XX:MaxGCPauseMillis=200","-XX:+AlwaysPreTouch",
    "-Ddisable.exit=true","-Dfile.encoding=UTF-8",
    "-Dreenc.mode=end","-Dreenc.minTouched=5000",
    "-Dreenc.batchSize=2000","-Dlog.progress.everyN=0"
)

$Batch = 100000

$Datasets = @(
    @{ Name="SIFT1M"; Base="E:\Research Work\Datasets\SIFT1M\sift_base.fvecs"; Query="E:\Research Work\Datasets\SIFT1M\sift_query.fvecs"; GT="E:\Research Work\Datasets\SIFT1M\sift_query_groundtruth.ivecs"; Dim=128 },
    @{ Name="Glove-100"; Base="E:\Research Work\Datasets\glove-100\glove-100_base.fvecs"; Query="E:\Research Work\Datasets\glove-100\glove-100_query.fvecs"; GT="E:\Research Work\Datasets\glove-100\glove-100_groundtruth.ivecs"; Dim=100 }
    @{ Name="RedCaps"; Base="E:\Research Work\Datasets\redcaps\redcaps_base.fvecs"; Query="E:\Research Work\Datasets\redcaps\redcaps_query.fvecs"; GT="E:\Research Work\Datasets\redcaps\redcaps_groundtruth.ivecs"; Dim=512 }

)

foreach ($cfg in $Configs) {
    $CFG_NAME = $cfg.Name
    $ConfigPath = $cfg.Path
    $OutRoot = "G:\fsp-run\$CFG_NAME"
    New-Item -ItemType Directory -Force -Path $OutRoot | Out-Null

    Write-Host "========================================"
    Write-Host "CONFIG FAMILY: $CFG_NAME"
    Write-Host "========================================"

    $cfgObj = Get-Content $ConfigPath -Raw | ConvertFrom-Json
    $base = $cfgObj.base

    foreach ($ds in $Datasets) {
        $datasetRoot = Join-Path $OutRoot $ds.Name
        New-Item -ItemType Directory -Force -Path $datasetRoot | Out-Null

        foreach ($p in $cfgObj.profiles) {
            $runDir = Join-Path $datasetRoot $p.name
            New-Item -ItemType Directory -Force -Path $runDir | Out-Null

            $final = $base | ConvertTo-Json -Depth 64 | ConvertFrom-Json
            foreach ($k in $p.overrides.PSObject.Properties) {
                $final.$($k.Name) = $k.Value
            }

            $final.output.resultsDir = Join-Path $runDir "results"
            $final.ratio.source = "gt"
            $final.ratio.gtPath = $ds.GT
            $final | ConvertTo-Json -Depth 64 | Out-File "$runDir\config.json"

            & java @JvmArgs `
        "-Dcli.dataset=$($ds.Name)" `
        "-Dcli.profile=$($p.name)" `
        "-jar" $JarPath `
        "$runDir\config.json" `
        $ds.Base $ds.Query "$runDir\keys.blob" `
        $ds.Dim $runDir $ds.GT $Batch `
        | Tee-Object "$runDir\run.out.log"
        }
    }
}
