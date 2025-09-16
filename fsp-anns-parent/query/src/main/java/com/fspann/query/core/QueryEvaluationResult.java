package com.fspann.query.core;

import java.util.Objects;

public class QueryEvaluationResult {
    private final int topKRequested;
    private final int retrieved;
    private final double ratio;
    private final double recall;
    private final long timeMs;
    private final long insertTimeMs;
    private final int candidateCount;
    private final int tokenSizeBytes;
    private final int vectorDim;
    private final int totalFlushedPoints;
    private final int flushThreshold;

    public QueryEvaluationResult(int topKRequested,
                                 int retrieved,
                                 double ratio,
                                 double recall,
                                 long timeMs,
                                 long insertTimeMs,
                                 int candidateCount,
                                 int tokenSizeBytes,
                                 int vectorDim,
                                 int totalFlushedPoints,
                                 int flushThreshold) {
        this.topKRequested  = topKRequested;
        this.retrieved      = retrieved;
        this.ratio          = ratio;
        this.recall         = recall;
        this.timeMs         = timeMs;
        this.insertTimeMs   = insertTimeMs;
        this.candidateCount = candidateCount;
        this.tokenSizeBytes = tokenSizeBytes;
        this.vectorDim      = vectorDim;
        this.totalFlushedPoints = totalFlushedPoints;
        this.flushThreshold = flushThreshold;

        if (topKRequested <= 0) throw new IllegalArgumentException("topKRequested must be positive");
        if (retrieved < 0) throw new IllegalArgumentException("retrieved must be non-negative");
        if (ratio < 0) throw new IllegalArgumentException("ratio must be non-negative");
        if (recall < 0 || recall > 1) throw new IllegalArgumentException("recall must be between 0 and 1");
        if (timeMs < 0) throw new IllegalArgumentException("timeMs must be non-negative");
        if (insertTimeMs < 0) throw new IllegalArgumentException("insertTimeMs must be non-negative");
        if (candidateCount < 0) throw new IllegalArgumentException("candidateCount must be non-negative");
        if (tokenSizeBytes < 0) throw new IllegalArgumentException("tokenSizeBytes must be non-negative");
        if (totalFlushedPoints < 0) throw new IllegalArgumentException("totalFlushedPoints must be non-negative");
        if (flushThreshold < 0) throw new IllegalArgumentException("flushThreshold must be non-negative");
    }

    @Override
    public String toString() {
        return String.format(
                "TopK=%d, Retrieved=%d, Ratio=%.4f, Recall=%.4f, Time=%dms, InsertTime=%dms, Candidates=%d, TokenSize=%d, Dim=%d, Flushed=%d/%d",
                topKRequested, retrieved, ratio, recall, timeMs, insertTimeMs, candidateCount, tokenSizeBytes, vectorDim,
                totalFlushedPoints, flushThreshold
        );
    }

    public int getTopKRequested() { return topKRequested; }
    public int getRetrieved() { return retrieved; }
    public double getRatio() { return ratio; }
    public double getRecall() { return recall; }
    public long getTimeMs() { return timeMs; }
    public long getInsertTimeMs() { return insertTimeMs; }
    public int getCandidateCount() { return candidateCount; }
    public int getTokenSizeBytes() { return tokenSizeBytes; }
    public int getVectorDim() { return vectorDim; }
    public int getTotalFlushedPoints() { return totalFlushedPoints; }
    public int getFlushThreshold() { return flushThreshold; }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof QueryEvaluationResult that)) return false;
        return topKRequested == that.topKRequested
                && retrieved == that.retrieved
                && Double.compare(that.ratio, ratio) == 0
                && Double.compare(that.recall, recall) == 0
                && timeMs == that.timeMs
                && insertTimeMs == that.insertTimeMs
                && candidateCount == that.candidateCount
                && tokenSizeBytes == that.tokenSizeBytes
                && vectorDim == that.vectorDim
                && totalFlushedPoints == that.totalFlushedPoints
                && flushThreshold == that.flushThreshold;
    }
    @Override public int hashCode() {
        return Objects.hash(topKRequested, retrieved, ratio, recall, timeMs, insertTimeMs,
                candidateCount, tokenSizeBytes, vectorDim, totalFlushedPoints, flushThreshold);
    }
}
