package com.fspann.index.lsh;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * AdaptiveProbeScheduler – Dynamic Parameter Tuning
 *
 * Automatically adjusts LSH parameters based on query metrics to maintain
 * target candidate ratio and recall.
 *
 * Key features:
 *  • Sliding window statistics (last N queries)
 *  • Bucket width adaptation
 *  • Ratio target maintenance
 *  • Thread-safe concurrent operation
 *
 * @author FSP-ANNS Project
 * @version 2.0 (String IDs)
 */
public class AdaptiveProbeScheduler {

    private final MultiTableLSH lshIndex;
    private final RandomProjectionLSH hashFamily;
    private final int windowSize;

    // Metrics window (last N queries)
    private final ConcurrentLinkedDeque<QueryMetric> metricsWindow;

    // Target ratio
    private volatile double targetRatio = 1.2;

    // Tuning parameters
    private volatile double widthAdjustmentFactor = 0.95;
    private volatile int minQueriesToTune = 10;

    // Statistics
    private volatile int totalQueriesProcessed = 0;
    private volatile int totalAdjustments = 0;

    /**
     * Construct adaptive scheduler.
     *
     * @param lshIndex    MultiTableLSH instance to tune
     * @param hashFamily  RandomProjectionLSH instance (can be null)
     * @param windowSize  size of metrics window
     */
    public AdaptiveProbeScheduler(MultiTableLSH lshIndex,
                                  RandomProjectionLSH hashFamily,
                                  int windowSize) {
        this.lshIndex = Objects.requireNonNull(lshIndex, "lshIndex");
        this.hashFamily = hashFamily;
        this.windowSize = Math.max(5, windowSize);
        this.metricsWindow = new ConcurrentLinkedDeque<>();
    }

    /**
     * Adaptive query with automatic parameter tuning.
     *
     * @param query query vector
     * @return list of (vectorId, distance) pairs ranked by distance
     */
    public List<Map.Entry<String, Double>> adaptiveQuery(double[] query) {
        Objects.requireNonNull(query, "query");

        long startTime = System.nanoTime();

        // Get initial results with current parameters
        List<Map.Entry<String, Double>> results = lshIndex.query(query, 10);

        long elapsed = (System.nanoTime() - startTime) / 1_000_000; // ms

        // Record metric
        int numCandidates = results.size();
        double ratio = numCandidates / 10.0;
        recordMetric(new QueryMetric(numCandidates, 10, ratio, elapsed));

        // Try to tune parameters if we have enough data
        maybeAdjustParameters();

        return results;
    }

    /**
     * Adaptive query with custom topK.
     */
    public List<Map.Entry<String, Double>> adaptiveQuery(double[] query, int topK) {
        Objects.requireNonNull(query, "query");
        if (topK <= 0) throw new IllegalArgumentException("topK must be > 0");

        long startTime = System.nanoTime();

        // Get results
        List<Map.Entry<String, Double>> results = lshIndex.query(query, topK);

        long elapsed = (System.nanoTime() - startTime) / 1_000_000; // ms

        // Record metric
        int numCandidates = results.size();
        double ratio = (double) numCandidates / topK;
        recordMetric(new QueryMetric(numCandidates, topK, ratio, elapsed));

        // Try to tune
        maybeAdjustParameters();

        return results;
    }

    /**
     * Record a query metric.
     */
    private void recordMetric(QueryMetric metric) {
        metricsWindow.addLast(metric);

        // Keep window size bounded
        if (metricsWindow.size() > windowSize) {
            metricsWindow.removeFirst();
        }

        totalQueriesProcessed++;
    }

    /**
     * Maybe adjust parameters based on metrics.
     */
    private void maybeAdjustParameters() {
        if (metricsWindow.size() < minQueriesToTune) {
            return;
        }

        // Compute average ratio
        double avgRatio = metricsWindow.stream()
                .mapToDouble(m -> m.ratio)
                .average()
                .orElse(1.0);

        // Check if we should adjust
        if (Math.abs(avgRatio - targetRatio) > 0.15) {
            adjustBucketWidths(avgRatio);
        }
    }

    /**
     * Adjust bucket widths based on observed ratio.
     */
    private void adjustBucketWidths(double currentRatio) {
        if (hashFamily == null || !hashFamily.isInitialized()) {
            return;
        }

        // If ratio too high (too many candidates), increase width
        if (currentRatio > targetRatio * 1.2) {
            for (int t = 0; t < hashFamily.getNumTables(); t++) {
                double oldWidth = hashFamily.getBucketWidth(t);
                double newWidth = oldWidth * widthAdjustmentFactor;
                hashFamily.setBucketWidth(t, newWidth);
            }
            totalAdjustments++;
        }
        // If ratio too low (too few candidates), decrease width
        else if (currentRatio < targetRatio * 0.8) {
            for (int t = 0; t < hashFamily.getNumTables(); t++) {
                double oldWidth = hashFamily.getBucketWidth(t);
                double newWidth = oldWidth / widthAdjustmentFactor;
                hashFamily.setBucketWidth(t, newWidth);
            }
            totalAdjustments++;
        }
    }

    /**
     * Get current average ratio.
     */
    public double getCurrentAverageRatio() {
        return metricsWindow.stream()
                .mapToDouble(m -> m.ratio)
                .average()
                .orElse(1.0);
    }

    /**
     * Get current average latency (ms).
     */
    public double getCurrentAverageLatency() {
        return metricsWindow.stream()
                .mapToDouble(m -> m.latencyMs)
                .average()
                .orElse(0.0);
    }

    /**
     * Get tuning statistics.
     */
    public Map<String, Double> getTuningStatistics() {
        return Map.of(
                "avg_ratio", getCurrentAverageRatio(),
                "avg_latency_ms", getCurrentAverageLatency(),
                "total_queries", (double) totalQueriesProcessed,
                "total_adjustments", (double) totalAdjustments,
                "target_ratio", targetRatio,
                "window_size", (double) metricsWindow.size()
        );
    }

    /**
     * Set target candidate ratio.
     */
    public void setTargetRatio(double targetRatio) {
        if (targetRatio <= 1.0 || targetRatio > 10.0) {
            throw new IllegalArgumentException("Target ratio should be in (1.0, 10.0]");
        }
        this.targetRatio = targetRatio;
    }

    /**
     * Set bucket width adjustment factor (decay factor).
     */
    public void setWidthAdjustmentFactor(double factor) {
        if (factor <= 0 || factor >= 1.0) {
            throw new IllegalArgumentException("Factor should be in (0, 1.0)");
        }
        this.widthAdjustmentFactor = factor;
    }

    /**
     * Set minimum queries before tuning starts.
     */
    public void setMinQueriesToTune(int minQueries) {
        if (minQueries < 1) {
            throw new IllegalArgumentException("minQueries must be >= 1");
        }
        this.minQueriesToTune = minQueries;
    }

    /**
     * Clear metrics window.
     */
    public void clearMetrics() {
        metricsWindow.clear();
        totalQueriesProcessed = 0;
        totalAdjustments = 0;
    }

    /**
     * Get window size.
     */
    public int getWindowSize() {
        return windowSize;
    }

    /**
     * Get number of metrics in window.
     */
    public int getMetricsCount() {
        return metricsWindow.size();
    }

    @Override
    public String toString() {
        return String.format(
                "AdaptiveProbeScheduler{window=%d, target_ratio=%.2f, queries=%d, adjustments=%d}",
                windowSize, targetRatio, totalQueriesProcessed, totalAdjustments);
    }

    // ====================================================================
    // INNER CLASS: Query Metric
    // ====================================================================

    /**
     * Single query metric snapshot.
     */
    private static class QueryMetric {
        final int candidatesRetrieved;
        final int topK;
        final double ratio;
        final long latencyMs;

        QueryMetric(int candidatesRetrieved, int topK, double ratio, long latencyMs) {
            this.candidatesRetrieved = candidatesRetrieved;
            this.topK = topK;
            this.ratio = ratio;
            this.latencyMs = latencyMs;
        }

        @Override
        public String toString() {
            return String.format("QueryMetric{candidates=%d, topK=%d, ratio=%.3f, latency=%dms}",
                    candidatesRetrieved, topK, ratio, latencyMs);
        }
    }
}