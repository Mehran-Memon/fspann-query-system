package com.fspann.query.core;

/**
 * Immutable container for per-(query, K) evaluation metrics.
 *
 * It now supports:
 *  - Core ANN metrics: ratio@K, precision@K
 *  - Timing split: server, client, run, decrypt, insert
 *  - Candidate pipeline: candTotal, candKept, candDecrypted, candReturned
 *  - Storage / re-encryption metrics
 *  - Provenance: ratioDenomSource, tokenK, tokenKBase, qIndex, candMetricsMode
 */
public final class QueryEvaluationResult {

    // Core K metrics
    private final int topKRequested;
    private final int retrieved;
    private final double ratio;
    private final double precision;

    // Time metrics (ms)
    private final long timeMs;        // server-side time
    private final long clientTimeMs;  // client wall-clock
    private final long runTimeMs;     // end-to-end run time (optional)
    private final long decryptTimeMs; // pure decrypt/scoring time (optional)
    private final long insertTimeMs;  // last insert/flush time (optional)

    // Candidate pipeline
    private final int candTotal;      // total scanned candidates
    private final int candKept;       // kept after version/filter
    private final int candDecrypted;  // actually decrypted/scored
    private final int candReturned;   // returned to caller

    // Token/vector metadata
    private final int tokenSizeBytes;
    private final int vectorDim;
    private final int tokenK;
    private final int tokenKBase;
    private final int qIndexZeroBased;
    private final String candMetricsMode; // "full" / "partial" / etc.

    // Buffer / flush stats
    private final int totalFlushedPoints;
    private final int flushThreshold;

    // Re-encryption / touch metrics
    private final int touchedCount;
    private final int reencryptedCount;
    private final long reencTimeMs;
    private final long reencBytesDelta;
    private final long reencBytesAfter;
    private final String ratioDenomSource; // "gt", "base", "gt(auto)", "base(auto)", etc.

    // ---------------------------------------------------------------------
    // MAIN MODERN CTOR  (used by ForwardSecureANNSystem)
    // ---------------------------------------------------------------------

    public QueryEvaluationResult(
            int topKRequested,
            int retrieved,
            double ratio,
            double precision,
            long timeMs,              // server time
            long insertTimeMs,
            int candDecrypted,
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
            long clientTimeMs,
            int tokenK,
            int tokenKBase,
            int qIndexZeroBased,
            String candMetricsMode
    ) {
        this.topKRequested   = topKRequested;
        this.retrieved       = retrieved;
        this.ratio           = ratio;
        this.precision       = precision;
        this.timeMs          = timeMs;
        this.insertTimeMs    = insertTimeMs;
        this.candDecrypted   = candDecrypted;
        this.tokenSizeBytes  = tokenSizeBytes;
        this.vectorDim       = vectorDim;
        this.totalFlushedPoints = totalFlushedPoints;
        this.flushThreshold  = flushThreshold;
        this.touchedCount    = touchedCount;
        this.reencryptedCount = reencryptedCount;
        this.reencTimeMs     = reencTimeMs;
        this.reencBytesDelta = reencBytesDelta;
        this.reencBytesAfter = reencBytesAfter;
        this.ratioDenomSource = ratioDenomSource;
        this.clientTimeMs    = clientTimeMs;
        this.tokenK          = tokenK;
        this.tokenKBase      = tokenKBase;
        this.qIndexZeroBased = qIndexZeroBased;
        this.candMetricsMode = candMetricsMode;

        // Fields not supplied by this ctor get sensible defaults
        this.candTotal       = -1;
        this.candKept        = -1;
        this.candReturned    = retrieved; // by default, returned == retrieved
        this.runTimeMs       = -1L;
        this.decryptTimeMs   = -1L;
    }

    // ---------------------------------------------------------------------
    // LEGACY CTOR  (16-arg version – used by your older runners)
    // ---------------------------------------------------------------------

    public QueryEvaluationResult(
            int topKRequested,
            int retrieved,
            double ratio,
            double precision,
            long timeMs,
            long insertTimeMs,
            int candDecrypted,
            int tokenSizeBytes,
            int vectorDim,
            int totalFlushedPoints,
            int flushThreshold,
            int touchedCount,
            int reencryptedCount,
            long reencTimeMs,
            long reencBytesDelta,
            long reencBytesAfter
    ) {
        // Delegate to the main ctor with reasonable defaults
        this.topKRequested   = topKRequested;
        this.retrieved       = retrieved;
        this.ratio           = ratio;
        this.precision       = precision;
        this.timeMs          = timeMs;
        this.insertTimeMs    = insertTimeMs;
        this.candDecrypted   = candDecrypted;
        this.tokenSizeBytes  = tokenSizeBytes;
        this.vectorDim       = vectorDim;
        this.totalFlushedPoints = totalFlushedPoints;
        this.flushThreshold  = flushThreshold;
        this.touchedCount    = touchedCount;
        this.reencryptedCount = reencryptedCount;
        this.reencTimeMs     = reencTimeMs;
        this.reencBytesDelta = reencBytesDelta;
        this.reencBytesAfter = reencBytesAfter;

        // Legacy ctor didn’t know about these:
        this.ratioDenomSource = null;
        this.clientTimeMs    = -1L;
        this.tokenK          = topKRequested;
        this.tokenKBase      = topKRequested;
        this.qIndexZeroBased = -1;
        this.candMetricsMode = "legacy";

        this.candTotal       = -1;
        this.candKept        = -1;
        this.candReturned    = retrieved;
        this.runTimeMs       = -1L;
        this.decryptTimeMs   = -1L;
    }

    // ---------------------------------------------------------------------
    // OPTIONAL: future extended ctor (if you ever want to pass candTotal etc.)
    // ---------------------------------------------------------------------
    // You can add another constructor that includes candTotal, candKept,
    // candReturned, runTimeMs, decryptTimeMs if needed later.

    // ---------------------------------------------------------------------
    // Getters
    // ---------------------------------------------------------------------

    public int getTopKRequested()      { return topKRequested; }
    public int getRetrieved()          { return retrieved; }
    public double getRatio()           { return ratio; }
    public double getPrecision()       { return precision; }

    public long getTimeMs()            { return timeMs; }
    public long getClientTimeMs()      { return clientTimeMs; }
    public long getRunTimeMs()         { return runTimeMs; }
    public long getDecryptTimeMs()     { return decryptTimeMs; }
    public long getInsertTimeMs()      { return insertTimeMs; }

    public int getCandTotal()          { return candTotal; }
    public int getCandKept()           { return candKept; }
    public int getCandDecrypted()      { return candDecrypted; }
    public int getCandReturned()       { return candReturned; }

    public int getTokenSizeBytes()     { return tokenSizeBytes; }
    public int getVectorDim()          { return vectorDim; }

    public int getTotalFlushedPoints() { return totalFlushedPoints; }
    public int getFlushThreshold()     { return flushThreshold; }

    public int getTouchedCount()       { return touchedCount; }
    public int getReencryptedCount()   { return reencryptedCount; }
    public long getReencTimeMs()       { return reencTimeMs; }
    public long getReencBytesDelta()   { return reencBytesDelta; }
    public long getReencBytesAfter()   { return reencBytesAfter; }

    public String getRatioDenomSource(){ return ratioDenomSource; }
    public long getClientTimeMillis()  { return clientTimeMs; } // alias if needed

    public int getTokenK()             { return tokenK; }
    public int getTokenKBase()         { return tokenKBase; }
    public int getQIndexZeroBased()    { return qIndexZeroBased; }
    public String getCandMetricsMode() { return candMetricsMode; }
}
