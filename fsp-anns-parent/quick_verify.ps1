# Check what IDs are in the groundtruth file (first 10 queries, first 5 neighbors each)
$gtPath = "E:\Research Work\Datasets\SIFT1M\sift_query_groundtruth.ivecs"

# Read first few entries from ivecs file
$bytes = [System.IO.File]::ReadAllBytes($gtPath)
$offset = 0

Write-Host "First 5 queries from groundtruth:" -ForegroundColor Cyan
for ($q = 0; $q -lt 5; $q++) {
    # Read dimension (number of neighbors per query)
    $dim = [BitConverter]::ToInt32($bytes, $offset)
    $offset += 4

    Write-Host "Query $q (dim=$dim): " -NoNewline

    # Read first 5 neighbor IDs
    $neighbors = @()
    for ($i = 0; $i -lt [Math]::Min(5, $dim); $i++) {
        $id = [BitConverter]::ToInt32($bytes, $offset + ($i * 4))
        $neighbors += $id
    }
    Write-Host ($neighbors -join ", ")

    $offset += ($dim * 4)
}