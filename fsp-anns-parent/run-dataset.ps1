# ============================
# FSP-ANN (single dataset, multi-profile)
# PS 5.1 / ConstrainedLanguage safe
# - Per-run cleanup for metadata/points/results
# - Precision CSV enabled in config
# - GT is ALWAYS precomputed each run (passes "AUTO")
# ============================

# ---- required paths ----
$JarPath    = "F:\fspann-query-system\fsp-anns-parent\api\target\api-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
$ConfigPath = "F:\fspann-query-system\fsp-anns-parent\config\src\main\resources\config.json"
$OutRoot    = "G:\fsp-run"

# ---- dataset: SIFT1M (edit these to your actual files) ----
$Name  = "SIFT1M"
$Dim   = 128
$Base  = "E:\Research Work\Datasets\sift_dataset\sift_base.fvecs"
$Query = "E:\Research Work\Datasets\sift_dataset\sift_query.fvecs"
$GT    = "E:\Research Work\Datasets\sift_dataset\sift_groundtruth.ivecs"   # ignored; AUTO will be used

# app batch size arg
$Batch = 100000

# cleanup toggles
$CleanPerRun = $true
$CleanAllNow = $false

# ---------- helpers (PS5-safe) ----------
function To-Hashtable {
    param([Parameter(Mandatory=$true)]$o)
    if ($null -eq $o) { return $null }
    if ($o -is [hashtable]) { return $o }
    if ($o -is [System.Collections.IDictionary]) {
        $ht=@{}; foreach($k in $o.Keys){ $ht[$k] = To-Hashtable $o[$k] }; return $ht
    }
    if ($o -is [System.Collections.IEnumerable] -and -not ($o -is [string])) {
        $arr=@(); foreach($e in $o){ $arr += ,(To-Hashtable $e) }; return $arr
    }
    if ($o -is [pscustomobject]) {
        $ht=@{}; foreach($p in $o.PSObject.Properties){ $ht[$p.Name] = To-Hashtable $p.Value }; return $ht
    }
    return $o
}

function Copy-Hashtable {
    param([hashtable]$h)
    $out=@{}
    foreach($k in $h.Keys){
        $v = $h[$k]
        if ($v -is [hashtable]) { $out[$k] = Copy-Hashtable $v }
        elseif ($v -is [System.Collections.IEnumerable] -and -not ($v -is [string])) {
            $arr=@(); foreach($e in $v){ if ($e -is [hashtable]) { $arr += ,(Copy-Hashtable $e) } else { $arr += ,$e } }
            $out[$k] = $arr
        } else { $out[$k] = $v }
    }
    return $out
}

function Apply-Overrides {
    param([hashtable]$base,[hashtable]$ovr)
    $result = Copy-Hashtable $base
    foreach($k in $ovr.Keys){
        $ov = $ovr[$k]
        if ($result.ContainsKey($k) -and ($result[$k] -is [hashtable]) -and ($ov -is [hashtable])) {
            foreach($sub in $ov.Keys){ $result[$k][$sub] = $ov[$sub] }
        } else {
            $result[$k] = $ov
        }
    }
    return $result
}

function Safe-Resolve([string]$Path, [bool]$AllowMissing = $false) {
    try {
        if ($AllowMissing -and -not (Test-Path -LiteralPath $Path)) { return $Path }
        return (Resolve-Path -LiteralPath $Path).Path
    } catch {
        if ($AllowMissing) { return $Path } else { throw }
    }
}

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

function Clean-RunMetadata([string]$RunDir) {
    $paths = @(
        (Join-Path $RunDir "metadata"),
        (Join-Path $RunDir "points"),
        (Join-Path $RunDir "results")
    )
    foreach ($p in $paths) {
        if (Test-Path -LiteralPath $p) {
            Write-Host "Cleaning $p ..."
            Invoke-FastDelete $p
        }
        New-Item -ItemType Directory -Force -Path $p | Out-Null
    }
}

function Clean-AllUnderOutRoot([string]$OutRootPath, [string]$DatasetName) {
    if (-not (Test-Path -LiteralPath $OutRootPath)) { return }
    $pattern = Join-Path $OutRootPath $DatasetName
    $nodes = Get-ChildItem -LiteralPath $pattern -Recurse -Directory -ErrorAction SilentlyContinue |
            Where-Object { $_.Name -in @('metadata','points','results') }
    foreach ($n in $nodes) {
        Write-Host "Wiping $($n.FullName) ..."
        Invoke-FastDelete $n.FullName
    }
}

# ---------- STEP 0: optional global clean and exit ----------
if ($CleanAllNow) {
    New-Item -ItemType Directory -Force -Path $OutRoot | Out-Null
    Clean-AllUnderOutRoot -OutRootPath $OutRoot -DatasetName $Name
    Write-Host "Global clean completed under $OutRoot\$Name."
    return
}

# ---------- read config ----------
if (-not (Test-Path -LiteralPath $ConfigPath)) { throw "Config not found: $ConfigPath" }
$cfgObj   = (Get-Content -LiteralPath $ConfigPath -Raw) | ConvertFrom-Json
$baseObj  = $cfgObj.base
$profiles = $cfgObj.profiles
if ($null -eq $baseObj)  { throw "config.json must contain a 'base' object." }
if ($null -eq $profiles) { throw "config.json must contain a 'profiles' array." }
$baseHT = To-Hashtable $baseObj

New-Item -ItemType Directory -Force -Path $OutRoot | Out-Null

# ---------- run all profiles ----------
foreach ($p in $profiles) {
    $pHT = To-Hashtable $p
    if (-not $pHT.ContainsKey('name')) { continue }
    $label = [string]$pHT['name']
    $ovr   = @{}; if ($pHT.ContainsKey('overrides')) { $ovr = $pHT['overrides'] }

    $runDir = Join-Path $OutRoot ("$Name\" + $label)
    New-Item -ItemType Directory -Force -Path $runDir | Out-Null
    if ($CleanPerRun) { Clean-RunMetadata -RunDir $runDir }

    # merge config & force resultsDir + precision CSV
    $final = Apply-Overrides -base $baseHT -ovr $ovr
    if (-not $final.ContainsKey('output')) { $final['output'] = @{} }
    $final['output']['resultsDir'] = (Join-Path $runDir "results")
    if (-not $final.ContainsKey('eval')) { $final['eval'] = @{} }
    $final['eval']['computePrecision'] = $true
    $final['eval']['writeGlobalPrecisionCsv'] = $true

    $tmpConf = Join-Path $runDir "config.json"
    ($final | ConvertTo-Json -Depth 64) | Out-File -FilePath $tmpConf -Encoding utf8

    $keysFile = Join-Path $runDir "keystore.blob"

    # ---- FORCE PRECOMPUTE EVERY RUN ----
    $gtArg = "AUTO"   # Java main will recompute GT to <query>.ivecs and then use it

    # Build safe argument string for java (with quoting)
    $args = @(
        "-Dbase.path=$Base",
        "-jar", $JarPath,
        $tmpConf, $Base, $Query, $keysFile, "$Dim", $runDir, $gtArg, "$Batch"
    )
    $quotedArgs = @()
    foreach ($a in $args) {
        if ($a -match '\s') { $quotedArgs += '"' + ($a -replace '"','\"') + '"'
        } else { $quotedArgs += $a }
    }
    $argLine = [string]::Join(" ", $quotedArgs)

    # Launch
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = "java"
    $psi.Arguments = $argLine
    $psi.WorkingDirectory = $runDir
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError  = $true
    $psi.UseShellExecute = $false

    $pobj = New-Object System.Diagnostics.Process
    $pobj.StartInfo = $psi
    [void]$pobj.Start()
    $out = $pobj.StandardOutput.ReadToEnd()
    $err = $pobj.StandardError.ReadToEnd()
    $pobj.WaitForExit()

    $out | Out-File -FilePath (Join-Path $runDir "run.out.log") -Encoding utf8
    $err | Out-File -FilePath (Join-Path $runDir "run.err.log") -Encoding utf8

    if ($pobj.ExitCode -ne 0) {
        Write-Warning ("Run failed for profile {0} (exit={1}). See logs." -f $label, $pobj.ExitCode)
    } else {
        Write-Host ("Completed: {0} ({1})" -f $Name, $label)
    }

    # convenience copies
    $resultsDir = Join-Path $runDir "results"
    $rt = Join-Path $resultsDir "results_table.csv"
    $gp = Join-Path $resultsDir "global_precision.csv"
    if (Test-Path -LiteralPath $rt) { Copy-Item -LiteralPath $rt -Destination (Join-Path $runDir "results_table.csv") -Force }
    if (Test-Path -LiteralPath $gp) { Copy-Item -LiteralPath $gp -Destination (Join-Path $runDir "global_precision.csv") -Force }
}