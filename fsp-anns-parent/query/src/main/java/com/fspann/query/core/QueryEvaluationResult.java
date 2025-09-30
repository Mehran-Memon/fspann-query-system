package com.fspann.query.core;

import java.util.Objects;

public class QueryEvaluationResult {
    private final int topKRequested;
    private final int retrieved;
    private final double ratio;
    private final double precision;
    private final long timeMs;
    private final long insertTimeMs;
    private final int candidateCount;
    private final int tokenSizeBytes;
    private final int vectorDim;
    private final int totalFlushedPoints;
    private final int flushThreshold;
    private final String ratioDenomSource; // gt|base|none
    private final long clientTimeMs;
    private final int  tokenK;             // which K the token encodes (if any)
    private final int  tokenKBase;         // base token K when deriving per-K rows
    private final int  qIndexZeroBased;    // avoid 1-based off-by-one
    private final String candMetricsMode;  // full|partial

    // --- NEW: selective re-encryption metrics ---
    /** Unique candidate points touched during this query (post-lookup union size). */
    private final int touchedCount;

    /** Number of points that were actually re-encrypted (subset of touched). */
    private final int reencryptedCount;

    /** Time taken to re-encrypt the reencryptedCount points (ms). */
    private final long reencTimeMs;

    /** Optional: storage delta from this re-encryption action (bytes). */
    private final long reencBytesDelta; // may be negative (e.g., compaction)

    /** Optional: storage size after re-encryption completes (bytes). */
    private final long reencBytesAfter;

    // ---------------- Constructors ----------------

        /** Legacy 11-arg ctor (no re-enc stats, no clarifiers). */
        public QueryEvaluationResult(int topKRequested,
                                     int retrieved,
                                     double ratio,
                                     double precision,
                                     long timeMs,
                                     long insertTimeMs,
                                     int candidateCount,
                                     int tokenSizeBytes,
                                     int vectorDim,
                                     int totalFlushedPoints,
                                     int flushThreshold) {
            this(topKRequested, retrieved, ratio, precision, timeMs, insertTimeMs,
                    candidateCount, tokenSizeBytes, vectorDim, totalFlushedPoints, flushThreshold,
                    /*touchedCount*/ 0, /*reEncrypted*/ 0, /*reencTimeMs*/ 0L,
                    /*reencBytesDelta*/ 0L, /*reencBytesAfter*/ 0L,
                    /*ratioDenomSource*/ "none",
                    /*clientTimeMs*/ -1L,
                    /*tokenK*/ topKRequested,
                    /*tokenKBase*/ topKRequested,
                    /*qIndexZeroBased*/ -1,
                    /*candMetricsMode*/ "full");
        }

        /** 16-arg ctor (with re-enc stats), clarifiers defaulted. */
        public QueryEvaluationResult(int topKRequested,
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
                                     long reencBytesAfter) {
            this(topKRequested, retrieved, ratio, precision, timeMs, insertTimeMs,
                    candidateCount, tokenSizeBytes, vectorDim, totalFlushedPoints, flushThreshold,
                    touchedCount, reencryptedCount, reencTimeMs, reencBytesDelta, reencBytesAfter,
                    /*ratioDenomSource*/ "none",
                    /*clientTimeMs*/ -1L,
                    /*tokenK*/ topKRequested,
                    /*tokenKBase*/ topKRequested,
                    /*qIndexZeroBased*/ -1,
                    /*candMetricsMode*/ "full");
        }

        /** Full ctor (re-enc stats + clarifiers). */
        public QueryEvaluationResult(int topKRequested,
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
                                     String candMetricsMode) {
            this.topKRequested  = topKRequested;
            this.retrieved      = retrieved;
            this.ratio          = ratio;
            this.precision      = precision;
            this.timeMs         = timeMs;
            this.insertTimeMs   = insertTimeMs;
            this.candidateCount = candidateCount;
            this.tokenSizeBytes = tokenSizeBytes;
            this.vectorDim      = vectorDim;
            this.totalFlushedPoints = totalFlushedPoints;
            this.flushThreshold = flushThreshold;

            this.touchedCount     = touchedCount;
            this.reencryptedCount = reencryptedCount;
            this.reencTimeMs      = reencTimeMs;
            this.reencBytesDelta  = reencBytesDelta;   // may be negative (compaction)
            this.reencBytesAfter  = reencBytesAfter;

            // NEW
            this.ratioDenomSource = (ratioDenomSource == null) ? "none" : ratioDenomSource;
            this.clientTimeMs     = clientTimeMs;      // allow -1 meaning "unknown"
            this.tokenK           = tokenK;
            this.tokenKBase       = tokenKBase;
            this.qIndexZeroBased  = qIndexZeroBased;
            this.candMetricsMode  = (candMetricsMode == null || candMetricsMode.isBlank()) ? "full" : candMetricsMode;

            // --- validation ---
            if (topKRequested <= 0) throw new IllegalArgumentException("topKRequested must be positive");
            if (retrieved < 0) throw new IllegalArgumentException("retrieved must be non-negative");
            if (!Double.isNaN(ratio) && ratio < 0) throw new IllegalArgumentException("ratio must be non-negative");
            if (!Double.isNaN(precision) && (precision < 0 || precision > 1))
                throw new IllegalArgumentException("precision must be between 0 and 1");
            if (timeMs < 0) throw new IllegalArgumentException("timeMs must be non-negative");
            if (insertTimeMs < 0) throw new IllegalArgumentException("insertTimeMs must be non-negative");
            if (candidateCount < 0) throw new IllegalArgumentException("candidateCount must be non-negative");
            if (tokenSizeBytes < 0) throw new IllegalArgumentException("tokenSizeBytes must be non-negative");
            if (totalFlushedPoints < 0) throw new IllegalArgumentException("totalFlushedPoints must be non-negative");
            if (flushThreshold < 0) throw new IllegalArgumentException("flushThreshold must be non-negative");

            if (touchedCount < 0) throw new IllegalArgumentException("touchedCount must be non-negative");
            if (reencryptedCount < 0) throw new IllegalArgumentException("reencryptedCount must be non-negative");
            if (reencTimeMs < 0) throw new IllegalArgumentException("reencTimeMs must be non-negative");
            // reencBytesDelta may be negative
            if (reencBytesAfter < 0) throw new IllegalArgumentException("reencBytesAfter must be non-negative");
            if (reencryptedCount > touchedCount)
                throw new IllegalArgumentException("reencryptedCount cannot exceed touchedCount");
        }

    // NEW getters
    public String getRatioDenomSource() { return ratioDenomSource; }
    public long   getClientTimeMs()     { return clientTimeMs; }
    public int    getTokenK()           { return tokenK; }
    public int    getTokenKBase()       { return tokenKBase; }
    public int    getQIndexZeroBased()  { return qIndexZeroBased; }
    public String getCandMetricsMode()  { return candMetricsMode; }

    // toString() — append the new fields at the end for clarity
    @Override
    public String toString() {
        return String.format(
                "TopK=%d, Retrieved=%d, Ratio=%.4f, Precision=%.4f, ServerTime=%dms, ClientTime=%dms, InsertTime=%dms, " +
                        "Candidates=%d, TokenSize=%d, Dim=%d, Flushed=%d/%d, " +
                        "Touched=%d, Reenc=%d, ReencTime=%dms, ReencΔ=%dB, ReencAfter=%dB, " +
                        "RatioDenom=%s, TokenK=%d, TokenKBase=%d, qIndex0=%d, CandMetrics=%s",
                topKRequested, retrieved, ratio, precision, timeMs, clientTimeMs, insertTimeMs,
                candidateCount, tokenSizeBytes, vectorDim, totalFlushedPoints, flushThreshold,
                touchedCount, reencryptedCount, reencTimeMs, reencBytesDelta, reencBytesAfter,
                ratioDenomSource, tokenK, tokenKBase, qIndexZeroBased, candMetricsMode
        );
    }

    // ---------------- getters ----------------
    public int getTopKRequested() { return topKRequested; }
    public int getRetrieved() { return retrieved; }
    public double getRatio() { return ratio; }
    public double getPrecision() { return precision; }
    public long getTimeMs() { return timeMs; }
    public long getInsertTimeMs() { return insertTimeMs; }
    public int getCandidateCount() { return candidateCount; }
    public int getTokenSizeBytes() { return tokenSizeBytes; }
    public int getVectorDim() { return vectorDim; }
    public int getTotalFlushedPoints() { return totalFlushedPoints; }
    public int getFlushThreshold() { return flushThreshold; }

    // NEW
    public int  getTouchedCount()     { return touchedCount; }
    public int  getReencryptedCount() { return reencryptedCount; }
    public long getReencTimeMs()      { return reencTimeMs; }
    public long getReencBytesDelta()  { return reencBytesDelta; }
    public long getReencBytesAfter()  { return reencBytesAfter; }

    // ---------------- equals/hashCode ----------------
    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof QueryEvaluationResult that)) return false;
        return topKRequested == that.topKRequested
                && retrieved == that.retrieved
                && Double.compare(that.ratio, ratio) == 0
                && Double.compare(that.precision, precision) == 0
                && timeMs == that.timeMs
                && insertTimeMs == that.insertTimeMs
                && candidateCount == that.candidateCount
                && tokenSizeBytes == that.tokenSizeBytes
                && vectorDim == that.vectorDim
                && totalFlushedPoints == that.totalFlushedPoints
                && flushThreshold == that.flushThreshold
                && touchedCount == that.touchedCount
                && reencryptedCount == that.reencryptedCount
                && reencTimeMs == that.reencTimeMs
                && reencBytesDelta == that.reencBytesDelta
                && reencBytesAfter == that.reencBytesAfter
                && clientTimeMs == that.clientTimeMs
                && tokenK == that.tokenK
                && tokenKBase == that.tokenKBase
                && qIndexZeroBased == that.qIndexZeroBased
                && Objects.equals(ratioDenomSource, that.ratioDenomSource)
                && Objects.equals(candMetricsMode, that.candMetricsMode);
    }

    @Override public int hashCode() {
        return Objects.hash(
                topKRequested, retrieved, ratio, precision, timeMs, insertTimeMs,
                candidateCount, tokenSizeBytes, vectorDim, totalFlushedPoints, flushThreshold,
                touchedCount, reencryptedCount, reencTimeMs, reencBytesDelta, reencBytesAfter,
                ratioDenomSource, clientTimeMs, tokenK, tokenKBase, qIndexZeroBased, candMetricsMode
        );
    }
}
