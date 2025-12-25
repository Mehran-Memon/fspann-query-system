<#
    FSP-ANN Diagnostic Script
    Run this BEFORE another smoke test to identify the exact issue
#>

param(
    [string]$ProjectRoot = "F:\fspann-query-system\fsp-anns-parent"
)

$ErrorActionPreference = "Stop"
Write-Host "=" * 60 -ForegroundColor Cyan
Write-Host "  FSP-ANN DIAGNOSTIC SCRIPT" -ForegroundColor Cyan
Write-Host "=" * 60 -ForegroundColor Cyan

# ============================================================
# TEST 1: Verify source file has MSB-first fix
# ============================================================
Write-Host "`n[TEST 1] Checking Coding.java source..." -ForegroundColor Yellow

$codingPath = "$ProjectRoot\index\src\main\java\com\fspann\index\paper\Coding.java"
$codingContent = Get-Content $codingPath -Raw

if ($codingContent -match "for\s*\(\s*int\s+i\s*=\s*G\.lambda\s*-\s*1\s*;\s*i\s*>=\s*0\s*;\s*i--") {
    Write-Host "  [PASS] Source has MSB-first loop (lambda-1 down to 0)" -ForegroundColor Green
} elseif ($codingContent -match "for\s*\(\s*int\s+i\s*=\s*0\s*;\s*i\s*<\s*G\.lambda\s*;\s*i\+\+") {
    Write-Host "  [FAIL] Source has LSB-first loop (0 up to lambda) - FIX NOT APPLIED!" -ForegroundColor Red
    exit 1
} else {
    Write-Host "  [WARN] Could not detect loop pattern" -ForegroundColor Yellow
}

# ============================================================
# TEST 2: Check if shaded JAR exists and when it was built
# ============================================================
Write-Host "`n[TEST 2] Checking shaded JAR..." -ForegroundColor Yellow

$jarPath = "$ProjectRoot\api\target\api-0.0.1-SNAPSHOT-shaded.jar"
if (Test-Path $jarPath) {
    $jarInfo = Get-Item $jarPath
    Write-Host "  JAR exists: $jarPath" -ForegroundColor Green
    Write-Host "  Last modified: $($jarInfo.LastWriteTime)" -ForegroundColor Cyan
    Write-Host "  Size: $([math]::Round($jarInfo.Length / 1MB, 2)) MB" -ForegroundColor Cyan
} else {
    Write-Host "  [FAIL] Shaded JAR not found!" -ForegroundColor Red
    exit 1
}

# ============================================================
# TEST 3: Extract and verify Coding.class from JAR
# ============================================================
Write-Host "`n[TEST 3] Extracting Coding.class from JAR..." -ForegroundColor Yellow

$tempDir = "$ProjectRoot\temp-diagnostic"
if (Test-Path $tempDir) { Remove-Item -Recurse -Force $tempDir }
New-Item -ItemType Directory -Path $tempDir | Out-Null

Push-Location $tempDir
try {
    # Extract just the Coding.class file
    jar -xf $jarPath "com/fspann/index/paper/Coding.class"

    if (Test-Path "com/fspann/index/paper/Coding.class") {
        Write-Host "  [PASS] Coding.class extracted from JAR" -ForegroundColor Green

        # Disassemble to check bytecode
        $bytecode = javap -c "com/fspann/index/paper/Coding" 2>&1
        $bytecode | Out-File "Coding-bytecode.txt"

        # Look for the loop pattern in bytecode
        # MSB-first should have: iconst_1, isub (for lambda-1), then loop with decrement
        # LSB-first would have: iconst_0, then loop with increment

        $bytecodeStr = $bytecode -join "`n"

        # Check for the C method
        if ($bytecodeStr -match "public static java\.util\.BitSet C") {
            Write-Host "  Found C() method in bytecode" -ForegroundColor Cyan

            # Save relevant portion
            Write-Host "`n  Bytecode snippet (search for loop direction):" -ForegroundColor Yellow
            # Find the C method and show a portion
            $lines = $bytecode
            $inMethod = $false
            $methodLines = @()
            foreach ($line in $lines) {
                if ($line -match "public static java\.util\.BitSet C") {
                    $inMethod = $true
                }
                if ($inMethod) {
                    $methodLines += $line
                    if ($methodLines.Count -gt 50) { break }
                }
            }
            $methodLines | ForEach-Object { Write-Host "    $_" }
        }
    } else {
        Write-Host "  [FAIL] Could not extract Coding.class" -ForegroundColor Red
    }
} finally {
    Pop-Location
}

# ============================================================
# TEST 4: Run inline verification using the SHADED JAR
# ============================================================
Write-Host "`n[TEST 4] Running bytecode verification against shaded JAR..." -ForegroundColor Yellow

$testJava = @"
import com.fspann.index.paper.Coding;
import java.util.BitSet;

public class BytecodeTest {
    public static void main(String[] args) {
        // Create test vector
        double[] v = new double[128];
        for(int i = 0; i < 128; i++) v[i] = i * 0.01 + 0.5;

        // Build GFunction with known seed
        Coding.GFunction G = Coding.buildRandomG(128, 24, 2, 1.0, 12345L);

        // Generate code
        BitSet code = Coding.C(v, G);
        int[] H = Coding.H(v, G);

        // For lambda=2, MSB is bit 1 of each H[j]
        // With MSB-first (CORRECT): code bit 0 = H[0] bit 1 (MSB)
        // With LSB-first (WRONG):   code bit 0 = H[0] bit 0 (LSB)

        boolean codeBit0 = code.get(0);
        boolean h0_MSB = ((H[0] >>> 1) & 1) == 1;  // bit 1 = MSB for lambda=2
        boolean h0_LSB = (H[0] & 1) == 1;          // bit 0 = LSB

        System.out.println("=== BYTECODE VERIFICATION ===");
        System.out.println("H[0] = " + H[0] + " (binary: " + Integer.toBinaryString(H[0] & 0x3) + ")");
        System.out.println("H[0] MSB (bit 1) = " + h0_MSB);
        System.out.println("H[0] LSB (bit 0) = " + h0_LSB);
        System.out.println("Code bit 0      = " + codeBit0);
        System.out.println();

        if (codeBit0 == h0_MSB) {
            System.out.println("RESULT: MSB-FIRST (CORRECT!)");
            System.out.println("The shaded JAR has the fix properly applied.");
            System.exit(0);
        } else if (codeBit0 == h0_LSB) {
            System.out.println("RESULT: LSB-FIRST (WRONG!)");
            System.out.println("The shaded JAR does NOT have the fix!");
            System.out.println("You need to rebuild: mvn clean install -U");
            System.exit(1);
        } else {
            System.out.println("RESULT: UNEXPECTED - neither MSB nor LSB match");
            System.exit(2);
        }
    }
}
"@

$testJava | Out-File -FilePath "$tempDir\BytecodeTest.java" -Encoding UTF8

Push-Location $tempDir
try {
    Write-Host "  Compiling test..." -ForegroundColor Cyan
    $compileResult = javac -cp $jarPath BytecodeTest.java 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  [FAIL] Compilation failed:" -ForegroundColor Red
        Write-Host $compileResult
        exit 1
    }

    Write-Host "  Running test against shaded JAR..." -ForegroundColor Cyan
    $runResult = java -cp "$jarPath;." BytecodeTest 2>&1
    Write-Host ""
    $runResult | ForEach-Object { Write-Host "  $_" -ForegroundColor White }

    if ($LASTEXITCODE -eq 0) {
        Write-Host "`n  [PASS] Shaded JAR has correct MSB-first ordering!" -ForegroundColor Green
    } else {
        Write-Host "`n  [FAIL] Shaded JAR has WRONG bit ordering!" -ForegroundColor Red
        exit 1
    }
} finally {
    Pop-Location
}

# ============================================================
# TEST 5: Check ground truth file
# ============================================================
Write-Host "`n[TEST 5] Checking ground truth setup..." -ForegroundColor Yellow

$gtPath = "E:\Research Work\Datasets\SIFT1M\sift_groundtruth.ivecs"
if (Test-Path $gtPath) {
    $gtInfo = Get-Item $gtPath
    Write-Host "  [PASS] Ground truth file exists: $gtPath" -ForegroundColor Green
    Write-Host "  Size: $([math]::Round($gtInfo.Length / 1MB, 2)) MB" -ForegroundColor Cyan

    # Check if it's the right size (10000 queries * 100 neighbors * 4 bytes per int + headers)
    $expectedMinSize = 10000 * 100 * 4
    if ($gtInfo.Length -ge $expectedMinSize) {
        Write-Host "  Size looks correct for 10K queries x 100 neighbors" -ForegroundColor Green
    } else {
        Write-Host "  [WARN] File seems too small" -ForegroundColor Yellow
    }
} else {
    Write-Host "  [WARN] Ground truth file not found at: $gtPath" -ForegroundColor Yellow
    Write-Host "  Precision calculation may be broken!" -ForegroundColor Yellow
}

# ============================================================
# TEST 6: Check config consistency
# ============================================================
Write-Host "`n[TEST 6] Checking configuration..." -ForegroundColor Yellow

$configPath = "G:\SMOKE_TEST\SIFT1M_M24\config.json"
if (Test-Path $configPath) {
    $config = Get-Content $configPath | ConvertFrom-Json
    Write-Host "  Config file: $configPath" -ForegroundColor Cyan

    if ($config.paper) {
        Write-Host "  paper.m = $($config.paper.m)" -ForegroundColor White
        Write-Host "  paper.lambda = $($config.paper.lambda)" -ForegroundColor White
        Write-Host "  paper.divisions = $($config.paper.divisions)" -ForegroundColor White
        Write-Host "  paper.tables = $($config.paper.tables)" -ForegroundColor White
        Write-Host "  paper.seed = $($config.paper.seed)" -ForegroundColor White
    }
} else {
    Write-Host "  [WARN] Config file not found" -ForegroundColor Yellow
}

# ============================================================
# SUMMARY
# ============================================================
Write-Host "`n" + ("=" * 60) -ForegroundColor Cyan
Write-Host "  DIAGNOSTIC SUMMARY" -ForegroundColor Cyan
Write-Host "=" * 60 -ForegroundColor Cyan

Write-Host @"

If TEST 4 passed (MSB-FIRST CORRECT), then the code is correct and
the problem is likely:
  1. Ground truth file missing or corrupt
  2. Precision calculation bug
  3. Index/query parameter mismatch

If TEST 4 failed (LSB-FIRST WRONG), then:
  1. The shaded JAR wasn't rebuilt properly
  2. Run: mvn clean install -U
  3. Delete old index: Remove-Item -Recurse G:\SMOKE_TEST\SIFT1M_M24\*
  4. Re-run smoke test

Diagnostic files saved to: $tempDir
"@ -ForegroundColor White

# Cleanup option
Write-Host "`nClean up temp directory? (y/n): " -NoNewline
$cleanup = Read-Host
if ($cleanup -eq 'y') {
    Remove-Item -Recurse -Force $tempDir
    Write-Host "Cleaned up." -ForegroundColor Green
}