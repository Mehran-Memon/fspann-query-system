<#
.SYNOPSIS
    FSP-ANN Complete Fix & Run - EVERYTHING IN ONE GO
.DESCRIPTION
    1. Verifies source fix is present
    2. Cleans and rebuilds shaded JAR
    3. Verifies bytecode has the fix
    4. Deletes old index
    5. Runs smoke test
#>

param(
    [string]$ProjectRoot = "F:\fspann-query-system\fsp-anns-parent",
    [string]$TestDir = "G:\SMOKE_TEST\SIFT1M_M24",
    [string]$DatasetPath = "E:\Research Work\Datasets\SIFT1M"
)

$ErrorActionPreference = "Stop"
Set-Location $ProjectRoot

Write-Host @"
╔══════════════════════════════════════════════════════════════╗
║       FSP-ANN COMPLETE FIX & RUN SCRIPT                      ║
║       This will rebuild everything from scratch              ║
╚══════════════════════════════════════════════════════════════╝
"@ -ForegroundColor Cyan

# ============================================================
# STEP 1: Verify source has MSB-first fix
# ============================================================
Write-Host "`n[STEP 1/6] Verifying Coding.java source..." -ForegroundColor Yellow

$codingPath = "$ProjectRoot\index\src\main\java\com\fspann\index\paper\Coding.java"
$content = Get-Content $codingPath -Raw

if ($content -match "for\s*\(\s*int\s+i\s*=\s*G\.lambda\s*-\s*1\s*;\s*i\s*>=\s*0\s*;\s*i--\s*\)") {
    Write-Host "  ✓ Source has MSB-first loop" -ForegroundColor Green
} else {
    Write-Host "  ✗ Source missing MSB-first fix! Applying now..." -ForegroundColor Red

    # Apply the fix automatically
    $fixed = $content -replace `
        'for\s*\(\s*int\s+i\s*=\s*0\s*;\s*i\s*<\s*G\.lambda\s*;\s*i\+\+\s*\)', `
        'for (int i = G.lambda - 1; i >= 0; i--)'

    $fixed | Set-Content $codingPath -Encoding UTF8
    Write-Host "  ✓ Fix applied to source" -ForegroundColor Green
}

# ============================================================
# STEP 2: Clean Maven build
# ============================================================
Write-Host "`n[STEP 2/6] Cleaning Maven build..." -ForegroundColor Yellow

# Delete all target directories
Get-ChildItem -Path $ProjectRoot -Recurse -Directory -Filter "target" |
        ForEach-Object {
            Write-Host "  Removing: $($_.FullName)" -ForegroundColor Gray
            Remove-Item -Recurse -Force $_.FullName -ErrorAction SilentlyContinue
        }

# Also delete Maven cache for this project (force fresh compile)
$m2Cache = "$env:USERPROFILE\.m2\repository\com\fspann"
if (Test-Path $m2Cache) {
    Write-Host "  Removing M2 cache: $m2Cache" -ForegroundColor Gray
    Remove-Item -Recurse -Force $m2Cache -ErrorAction SilentlyContinue
}

Write-Host "  ✓ Clean complete" -ForegroundColor Green

# ============================================================
# STEP 3: Maven build
# ============================================================
Write-Host "`n[STEP 3/6] Building with Maven (this takes ~2 min)..." -ForegroundColor Yellow

$buildStart = Get-Date
mvn clean install -DskipTests -q 2>&1 | Out-Null

if ($LASTEXITCODE -ne 0) {
    Write-Host "  ✗ Maven build failed! Running verbose build..." -ForegroundColor Red
    mvn clean install -DskipTests
    exit 1
}

$buildTime = (Get-Date) - $buildStart
Write-Host "  ✓ Build complete in $([math]::Round($buildTime.TotalSeconds))s" -ForegroundColor Green

# ============================================================
# STEP 4: Verify shaded JAR has the fix
# ============================================================
Write-Host "`n[STEP 4/6] Verifying shaded JAR bytecode..." -ForegroundColor Yellow

$jarPath = "$ProjectRoot\api\target\api-0.0.1-SNAPSHOT-shaded.jar"
if (-not (Test-Path $jarPath)) {
    Write-Host "  ✗ Shaded JAR not found at: $jarPath" -ForegroundColor Red
    exit 1
}

# Create temp verification
$tempDir = "$ProjectRoot\temp_verify"
New-Item -ItemType Directory -Path $tempDir -Force | Out-Null

$verifyJava = @'
import com.fspann.index.paper.Coding;
import java.util.BitSet;

public class Verify {
    public static void main(String[] args) {
        double[] v = new double[128];
        for(int i = 0; i < 128; i++) v[i] = i * 0.01 + 0.5;

        Coding.GFunction G = Coding.buildRandomG(128, 24, 2, 1.0, 12345L);
        BitSet code = Coding.C(v, G);
        int[] H = Coding.H(v, G);

        // For lambda=2: MSB is bit 1, LSB is bit 0
        boolean codeBit0 = code.get(0);
        boolean h0_MSB = ((H[0] >>> 1) & 1) == 1;

        if (codeBit0 == h0_MSB) {
            System.out.println("VERIFIED: MSB-FIRST (CORRECT)");
            System.exit(0);
        } else {
            System.out.println("FAILED: LSB-FIRST (WRONG) - JAR not rebuilt properly!");
            System.exit(1);
        }
    }
}
'@

$verifyJava | Out-File "$tempDir\Verify.java" -Encoding UTF8

Push-Location $tempDir
try {
    javac -cp $jarPath Verify.java 2>&1 | Out-Null
    $result = java -cp "$jarPath;." Verify 2>&1

    if ($LASTEXITCODE -eq 0) {
        Write-Host "  ✓ $result" -ForegroundColor Green
    } else {
        Write-Host "  ✗ $result" -ForegroundColor Red
        Write-Host "  The shaded JAR still has old code! This should not happen." -ForegroundColor Red
        exit 1
    }
} finally {
    Pop-Location
    Remove-Item -Recurse -Force $tempDir -ErrorAction SilentlyContinue
}

# ============================================================
# STEP 5: Delete old index (CRITICAL!)
# ============================================================
Write-Host "`n[STEP 5/6] Deleting old index data..." -ForegroundColor Yellow

if (Test-Path $TestDir) {
    Write-Host "  Removing: $TestDir" -ForegroundColor Gray
    Remove-Item -Recurse -Force $TestDir
}
New-Item -ItemType Directory -Path $TestDir -Force | Out-Null
New-Item -ItemType Directory -Path "$TestDir\results" -Force | Out-Null

Write-Host "  ✓ Old index deleted, fresh directory created" -ForegroundColor Green

# ============================================================
# STEP 6: Create config and run
# ============================================================
Write-Host "`n[STEP 6/6] Creating config and starting test..." -ForegroundColor Yellow

$config = @{
    paperMode = $true
    searchMode = "PARTITIONED_REAL"
    vectorPath = "$DatasetPath\sift_base.fvecs"
    queryPath = "$DatasetPath\sift_query.fvecs"
    groundTruthPath = "$DatasetPath\sift_groundtruth.ivecs"
    outputDir = $TestDir
    paper = @{
        m = 24
        lambda = 2
        divisions = 3
        tables = 1
        seed = 42
        omega = 4.0
        safetyMaxCandidates = 50000
    }
    runtime = @{
        maxCandidateFactor = 50
        maxRefinementFactor = 10
        maxRelaxationDepth = 2
        earlyStopCandidates = 0
    }
    stabilization = @{
        enabled = $true
        alpha = 0.1
        minCandidatesRatio = 1.5
    }
    indexThreads = 8
    queryThreads = 4
    maxQueries = 400
    topK = 100
    crypto = @{
        algorithm = "AES/GCM/NoPadding"
        keySize = 256
    }
} | ConvertTo-Json -Depth 10

$config | Out-File "$TestDir\config.json" -Encoding UTF8
Write-Host "  Config written to: $TestDir\config.json" -ForegroundColor Cyan

# Show config summary
Write-Host @"

  Configuration Summary:
  ----------------------
  paper.m = 24 (projections)
  paper.lambda = 2 (bits per projection)
  paper.divisions = 3
  paper.tables = 1
  Code bits = 24 * 2 = 48 per division

  Dataset: SIFT1M (1M vectors, 128-dim)
  Queries: 400
  TopK: 100

"@ -ForegroundColor White

Write-Host "Starting smoke test..." -ForegroundColor Yellow
Write-Host "=" * 60 -ForegroundColor Cyan

$startTime = Get-Date

# Run the actual test
java -Xmx16g -jar $jarPath `
    --config "$TestDir\config.json" `
    2>&1 | Tee-Object -FilePath "$TestDir\run.log"

$endTime = Get-Date
$duration = $endTime - $startTime

Write-Host "`n" + ("=" * 60) -ForegroundColor Cyan
Write-Host "Test completed in $([math]::Round($duration.TotalMinutes, 1)) minutes" -ForegroundColor Cyan

# Check results
if (Test-Path "$TestDir\results\profiler_metrics.csv") {
    Write-Host "`nResults Summary:" -ForegroundColor Yellow
    $metrics = Import-Csv "$TestDir\results\profiler_metrics.csv"

    $avgPrecision = ($metrics | Measure-Object -Property precision -Average).Average
    $avgRatio = ($metrics | Measure-Object -Property ratio -Average).Average

    Write-Host "  Average Precision: $([math]::Round($avgPrecision, 4))" -ForegroundColor $(if ($avgPrecision -gt 0.1) { "Green" } else { "Red" })
    Write-Host "  Average Ratio: $([math]::Round($avgRatio, 3))" -ForegroundColor White

    if ($avgPrecision -lt 0.01) {
        Write-Host "`n  ⚠ PRECISION STILL NEAR ZERO - Check ground truth file!" -ForegroundColor Red
    } elseif ($avgPrecision -gt 0.5) {
        Write-Host "`n  ✓ SUCCESS! Fix is working!" -ForegroundColor Green
    }
}

Write-Host "`nLog file: $TestDir\run.log" -ForegroundColor Gray