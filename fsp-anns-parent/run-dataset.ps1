# ============================
# FSP-ANN: SIFT1M × (throughput | balanced | recall_first)
# PS 5.1 & ConstrainedLanguage safe (no &, no ?., no ternary)
# Only passes -Dbase.path (everything else comes from config)
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
$GT    = "E:\Research Work\Datasets\sift_dataset\sift_groundtruth.ivecs"

# app batch size arg
$Batch = 100000

# ---------- helpers (PS5-safe) ----------
function To-Hashtable {
    param([Parameter(Mandatory=$true)]$o)
    if ($null -eq $o) { return $null }
    if ($o -is [hashtable]) { return $o }
    if ($o -is [System.Collections.IDictionary]) {
        $ht = @{}; foreach ($k in $o.Keys) { $ht[$k] = To-Hashtable $o[$k] }; return $ht
    }
    if ($o -is [System.Collections.IEnumerable] -and -not ($o -is [string])) {
        $arr = @(); foreach ($e in $o) { $arr += ,(To-Hashtable $e) }; return $arr
    }
    if ($o -is [pscustomobject]) {
        $ht = @{}; foreach ($p in $o.PSObject.Properties) { $ht[$p.Name] = To-Hashtable $p.Value }; return $ht
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

# ---------- read config (expects: { "base": {...}, "profiles": [ {name, overrides}, ... ] } ) ----------
if (-not (Test-Path -LiteralPath $ConfigPath)) { throw "Config not found: $ConfigPath" }
$cfgObj   = (Get-Content -LiteralPath $ConfigPath -Raw) | ConvertFrom-Json
$baseObj  = $cfgObj.base
$profiles = $cfgObj.profiles
if ($null -eq $baseObj)  { throw "config.json must contain a 'base' object." }
if ($null -eq $profiles) { throw "config.json must contain a 'profiles' array." }

$baseHT = To-Hashtable $baseObj

# ---------- run all profiles ----------
foreach ($p in $profiles) {
    $pHT = To-Hashtable $p
    if (-not $pHT.ContainsKey('name')) { continue }
    $label = [string]$pHT['name']
    $ovr   = @{}; if ($pHT.ContainsKey('overrides')) { $ovr = $pHT['overrides'] }

    $final = Apply-Overrides -base $baseHT -ovr $ovr

    $runDir = Join-Path $OutRoot ("$Name\" + $label)
    New-Item -ItemType Directory -Force -Path $runDir | Out-Null

    $tmpConf = Join-Path $runDir "config.json"
    ($final | ConvertTo-Json -Depth 64) | Out-File -FilePath $tmpConf -Encoding utf8

    $keysFile = Join-Path $runDir "keystore.blob"

    # Build safe argument string for java (with quoting)
    $args = @(
        "-Dbase.path=$Base",
        "-jar", $JarPath,
        $tmpConf, $Base, $Query, $keysFile, "$Dim", $runDir, $GT, "$Batch"
    )

    $quotedArgs = @()
    foreach ($a in $args) {
        if ($a -match '\s') {
            $quotedArgs += '"' + ($a -replace '"','\"') + '"'
        } else {
            $quotedArgs += $a
        }
    }
    $argLine = [string]::Join(" ", $quotedArgs)

    # Start java without using the & call operator
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
}
