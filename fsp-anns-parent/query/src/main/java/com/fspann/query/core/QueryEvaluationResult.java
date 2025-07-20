package com.fspann.query.core;

public class QueryEvaluationResult {
    private final int topKRequested;
    private final int retrieved;
    private final double ratio;
    private final double recall;  // ✅ New field
    private final long timeMs;

    public QueryEvaluationResult(int topKRequested, int retrieved, double ratio, double recall, long timeMs, int queryIndex, long timestamp)
{
        this.topKRequested = topKRequested;
        this.retrieved = retrieved;
        this.ratio = ratio;
        this.recall = recall;
        this.timeMs = timeMs;
    }

    public int getTopKRequested() { return topKRequested; }
    public int getRetrieved() { return retrieved; }
    public double getRatio() { return ratio; }
    public double getRecall() { return recall; }
    public long getTimeMs() { return timeMs; }

    @Override
    public String toString() {
        return String.format("TopK=%d, Retrieved=%d, Ratio=%.4f, Recall=%.4f, Time=%dms",
                topKRequested, retrieved, ratio, recall, timeMs);
    }
}
