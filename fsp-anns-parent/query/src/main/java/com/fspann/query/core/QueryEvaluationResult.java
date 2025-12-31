package com.fspann.query.core;

/**
 * Unified evaluation record for ANN search.
 * Supports:
 *  - SANNP ratio@K (GT-denominator)
 *  - PP-ANN precision@K (hit-rate)
 *  - Timing split: server/client/run/decrypt/insert
 *  - Candidate pipeline: total / kept / decrypted / returned
 *  - Re-encryption cost
 *  - Token metadata
 */
public final class QueryEvaluationResult {

    // ---------------- Core K metrics ----------------
    private final int topKRequested;
    private final int retrieved;
    private final double ratio;
    private final double recall;

    // ---------------- Timing (ms) ----------------
    private final long timeMs;          // server-only
    private final long clientTimeMs;    // outside-server
    private final long runTimeMs;       // (optional) end-to-end
    private final long decryptTimeMs;   // decrypt+score time
    private final long insertTimeMs;    // flush time

    // ---------------- Candidate pipeline ----------------
    private final int candTotal;        // scanned
    private final int candKept;         // version-kept
    private final int candDecrypted;    // AES-decrypted
    private final int candReturned;     // top-K returned

    // ---------------- Token/vector ----------------
    private final int tokenSizeBytes;
    private final int vectorDim;
    private final int tokenK;           // logical K
    private final int tokenKBase;       // original/base K
    private final int qIndexZeroBased;
    private final String candMetricsMode;

    // ---------------- Flush stats ----------------
    private final int totalFlushedPoints;
    private final int flushThreshold;

    // ---------------- Re-encryption ----------------
    private final int touchedCount;
    private final int reencryptedCount;
    private final long reencTimeMs;
    private final long reencBytesDelta;
    private final long reencBytesAfter;
    private final String ratioDenomSource;

    // =============================================================
    //          MAIN UNIFIED CONSTRUCTOR (FULL PIPELINE)
    // =============================================================

    public QueryEvaluationResult(
            int topKRequested,
            int retrieved,
            double ratio,
            double recall,
            long timeMs,
            long clientTimeMs,
            long runTimeMs,
            long decryptTimeMs,
            long insertTimeMs,
            int candTotal,
            int candKept,
            int candDecrypted,
            int candReturned,
            int tokenSizeBytes,
            int vectorDim,
            int totalFlushedPoints,
            int flushThreshold,
            int touchedCount,
            int reencryptedCount,
            long reencTimeMs,
            long reencBytesDelta,
            long reencBytesAfter,
            String ratioDenomSource,
            int tokenK,
            int tokenKBase,
            int qIndexZeroBased,
            String candMetricsMode
    ) {
        this.topKRequested = topKRequested;
        this.retrieved = retrieved;
        this.ratio = ratio;
        this.recall = recall;

        this.timeMs = timeMs;
        this.clientTimeMs = clientTimeMs;
        this.runTimeMs = runTimeMs;
        this.decryptTimeMs = decryptTimeMs;
        this.insertTimeMs = insertTimeMs;

        this.candTotal = candTotal;
        this.candKept = candKept;
        this.candDecrypted = candDecrypted;
        this.candReturned = candReturned;

        this.tokenSizeBytes = tokenSizeBytes;
        this.vectorDim = vectorDim;
        this.tokenK = tokenK;
        this.tokenKBase = tokenKBase;
        this.qIndexZeroBased = qIndexZeroBased;
        this.candMetricsMode = candMetricsMode;

        this.totalFlushedPoints = totalFlushedPoints;
        this.flushThreshold = flushThreshold;

        this.touchedCount = touchedCount;
        this.reencryptedCount = reencryptedCount;
        this.reencTimeMs = reencTimeMs;
        this.reencBytesDelta = reencBytesDelta;
        this.reencBytesAfter = reencBytesAfter;
        this.ratioDenomSource = ratioDenomSource;
    }

    // =============================================================
    //                       GETTERS
    // =============================================================
    public int getTopKRequested() { return topKRequested; }
    public int getRetrieved() { return retrieved; }
    public double getRatio() { return ratio; }
    public double getRecall() { return recall; }

    public long getTimeMs() { return timeMs; }
    public long getClientTimeMs() { return clientTimeMs; }
    public long getRunTimeMs() { return runTimeMs; }
    public long getDecryptTimeMs() { return decryptTimeMs; }
    public long getInsertTimeMs() { return insertTimeMs; }

    public int getCandTotal() { return candTotal; }
    public int getCandKept() { return candKept; }
    public int getCandDecrypted() { return candDecrypted; }
    public int getCandReturned() { return candReturned; }

    public int getTokenSizeBytes() { return tokenSizeBytes; }
    public int getVectorDim() { return vectorDim; }
    public int getTokenK() { return tokenK; }
    public int getTokenKBase() { return tokenKBase; }
    public int getQIndexZeroBased() { return qIndexZeroBased; }
    public String getCandMetricsMode() { return candMetricsMode; }

    public int getTotalFlushedPoints() { return totalFlushedPoints; }
    public int getFlushThreshold() { return flushThreshold; }

    public int getTouchedCount() { return touchedCount; }
    public int getReencryptedCount() { return reencryptedCount; }
    public long getReencTimeMs() { return reencTimeMs; }
    public long getReencBytesDelta() { return reencBytesDelta; }
    public long getReencBytesAfter() { return reencBytesAfter; }

    public String getRatioDenomSource() { return ratioDenomSource; }

    public int getSafeReencCount() {
        return Math.max(0, reencryptedCount);
    }

    public long getSafeReencTimeMs() {
        return Math.max(0, reencTimeMs);
    }

}
