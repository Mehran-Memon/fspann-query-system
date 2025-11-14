package com.fspann.query.core;

/**
 * Immutable per-query, per-K evaluation record.
 *
 * This class is intentionally "fat" so that we can export all the
 * interesting metrics (latency, precision, ratio, re-encryption, etc.)
 * without having to re-join multiple CSVs later.
 */
public class QueryEvaluationResult {
    private final int    topKRequested;      // K requested for this row
    private final int    retrieved;          // how many results actually returned (prefix size at K)
    private final double ratio;              // distance ratio@K (paper metric)
    private final double precision;          // precision@K

    private final long   timeMs;             // server-side time in ms (query layer)
    private final long   insertTimeMs;       // last batch insert time (ms), for correlation
    private final int    candidateCount;     // number of decrypted/scored candidates for this row

    private final int    tokenSizeBytes;     // serialized token size estimate
    private final int    vectorDim;          // dimension of vectors for this query
    private final int    totalFlushedPoints; // cumulative flushed points in buffer
    private final int    flushThreshold;     // flush threshold at time of query

    private final int    touchedCount;       // candidates "touched" by the query
    private final int    reencryptedCount;   // how many points re-encrypted (if any) for this query
    private final long   reencTimeMs;        // re-encryption time in ms for this query
    private final long   reencBytesDelta;    // size delta in bytes due to re-encryption
    private final long   reencBytesAfter;    // size after re-encryption in bytes

    private final String ratioDenomSource;   // "gt", "base", "gt(auto)", "base(auto)", "none", "test"
    private final long   clientTimeMs;       // client wall-clock time in ms

    private final int    tokenK;             // K used to build the query token
    private final int    tokenKBase;         // base K for the token (often same as tokenK)
    private final int    qIndexZeroBased;    // query index in [0..Q-1]
    private final String candMetricsMode;    // "full" or "partial" (if some candidate counters unavailable)

    public QueryEvaluationResult(
            int topKRequested,
            int retrieved,
            double ratio,
            double precision,
            long timeMs,
            long insertTimeMs,
            int candidateCount,
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
        this.topKRequested     = topKRequested;
        this.retrieved         = retrieved;
        this.ratio             = ratio;
        this.precision         = precision;
        this.timeMs            = timeMs;
        this.insertTimeMs      = insertTimeMs;
        this.candidateCount    = candidateCount;
        this.tokenSizeBytes    = tokenSizeBytes;
        this.vectorDim         = vectorDim;
        this.totalFlushedPoints = totalFlushedPoints;
        this.flushThreshold    = flushThreshold;
        this.touchedCount      = touchedCount;
        this.reencryptedCount  = reencryptedCount;
        this.reencTimeMs       = reencTimeMs;
        this.reencBytesDelta   = reencBytesDelta;
        this.reencBytesAfter   = reencBytesAfter;
        this.ratioDenomSource  = (ratioDenomSource == null ? "none" : ratioDenomSource);
        this.clientTimeMs      = clientTimeMs;
        this.tokenK            = tokenK;
        this.tokenKBase        = tokenKBase;
        this.qIndexZeroBased   = qIndexZeroBased;
        this.candMetricsMode   = (candMetricsMode == null ? "full" : candMetricsMode);
    }

    // --- Getters used across the system ---

    public int getTopKRequested()      { return topKRequested; }
    public int getRetrieved()          { return retrieved; }
    public double getRatio()           { return ratio; }
    public double getPrecision()       { return precision; }

    public long getTimeMs()            { return timeMs; }
    public long getInsertTimeMs()      { return insertTimeMs; }
    public int getCandidateCount()     { return candidateCount; }

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
    public long getClientTimeMs()      { return clientTimeMs; }

    public int getTokenK()             { return tokenK; }
    public int getTokenKBase()         { return tokenKBase; }
    public int getQIndexZeroBased()    { return qIndexZeroBased; }
    public String getCandMetricsMode() { return candMetricsMode; }

    // Optional helper for tests if you want:
    public static QueryEvaluationResult testSimple(int k, int retrieved, double precision) {
        return new QueryEvaluationResult(
                k, retrieved, Double.NaN, precision,
                0L, 0L, 0,
                0, 0, 0, 0,
                0, 0, 0L, 0L, 0L,
                "test", 0L,
                k, k, -1, "test"
        );
    }
}
