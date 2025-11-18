package com.fspann.common;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Simple profiler for measuring durations (in nanoseconds) and
 * logging per-query metrics such as server/client time and ratio.
 *
 * Thread-safety:
 *  - All mutating methods are synchronized.
 *  - Getter methods return defensive copies.
 */
public class Profiler {

    // ---- raw timings per label ----
    private final Map<String, Long> startTimes = new HashMap<>();
    private final Map<String, List<Long>> timings = new LinkedHashMap<>();

    // ---- per-query metrics for ART / ratio ----
    private final List<QueryMetric> queryMetrics = new ArrayList<>();
    private final List<Double> clientQueryTimes = new ArrayList<>();
    private final List<Double> serverQueryTimes = new ArrayList<>();
    private final List<Double> queryRatios       = new ArrayList<>();

    // Optional: generic rows for Top-K / debug tables
    private final List<String[]> topKRecords = new ArrayList<>();

    // ========================================================================
    // Timing API
    // ========================================================================

    /** Start a timer for the given label. */
    public synchronized void start(String label) {
        startTimes.put(label, System.nanoTime());
    }

    /**
     * Stop the timer for the given label and record the duration (ns).
     * If start() was never called, this is a no-op.
     */
    public synchronized void stop(String label) {
        Long start = startTimes.remove(label);
        if (start == null) {
            return; // no matching start; ignore
        }
        long end = System.nanoTime();
        long duration = Math.max(0L, end - start);
        timings
                .computeIfAbsent(label, k -> new ArrayList<>())
                .add(duration);
    }

    /**
     * Manually record a timing (in nanoseconds) against a label.
     * Useful if you already measured a duration elsewhere.
     */
    public synchronized void recordTiming(String label, long durationNs) {
        if (durationNs < 0) durationNs = 0;
        timings
                .computeIfAbsent(label, k -> new ArrayList<>())
                .add(durationNs);
    }

    /** Get a copy of the full timings map (label → list of durations in ns). */
    public synchronized Map<String, List<Long>> getTimings() {
        Map<String, List<Long>> copy = new LinkedHashMap<>();
        for (Map.Entry<String, List<Long>> e : timings.entrySet()) {
            copy.put(e.getKey(), new ArrayList<>(e.getValue()));
        }
        return copy;
    }

    /** Get all recorded durations (ns) for a specific label. */
    public synchronized List<Long> getTimings(String label) {
        List<Long> list = timings.get(label);
        return (list == null) ? Collections.emptyList() : new ArrayList<>(list);
    }

    /** Clear all timing, query, and Top-K data. */
    public synchronized void reset() {
        startTimes.clear();
        timings.clear();
        queryMetrics.clear();
        clientQueryTimes.clear();
        serverQueryTimes.clear();
        queryRatios.clear();
        topKRecords.clear();
    }

    // ========================================================================
    // Query-level metrics (for ART / ratio summaries)
    // ========================================================================

    /**
     * Record a single query metric.
     *
     * @param label     query identifier (e.g., "Q0")
     * @param serverMs  server-side time in milliseconds
     * @param clientMs  client-side time in milliseconds
     * @param ratio     accuracy ratio (may be NaN)
     */
    public synchronized void recordQueryMetric(String label,
                                               double serverMs,
                                               double clientMs,
                                               double ratio) {
        QueryMetric qm = new QueryMetric(label, serverMs, clientMs, ratio);
        queryMetrics.add(qm);
        serverQueryTimes.add(serverMs);
        clientQueryTimes.add(clientMs);
        queryRatios.add(ratio);
    }

    /** Copy of all recorded query metrics. */
    public synchronized List<QueryMetric> getQueryMetrics() {
        return new ArrayList<>(queryMetrics);
    }

    /** All client-side times (ms) across queries. */
    public synchronized List<Double> getAllClientQueryTimes() {
        return new ArrayList<>(clientQueryTimes);
    }

    /** All server-side times (ms) across queries. */
    public synchronized List<Double> getAllServerQueryTimes() {
        return new ArrayList<>(serverQueryTimes);
    }

    /** All recorded ratios across queries. */
    public synchronized List<Double> getAllQueryRatios() {
        return new ArrayList<>(queryRatios);
    }

    // ========================================================================
    // Top-K / arbitrary records (optional, for CSV tables)
    // ========================================================================

    /** Append an arbitrary CSV row (e.g., from Top-K evaluation). */
    public synchronized void addTopKRecord(String... cols) {
        if (cols != null) {
            topKRecords.add(Arrays.copyOf(cols, cols.length));
        }
    }

    /** Get a defensive copy of all stored Top-K rows. */
    public synchronized List<String[]> getTopKRecords() {
        List<String[]> out = new ArrayList<>(topKRecords.size());
        for (String[] row : topKRecords) {
            out.add(Arrays.copyOf(row, row.length));
        }
        return out;
    }

    // ========================================================================
    // CSV export helpers
    // ========================================================================

    /**
     * Export per-query metrics to a CSV file:
     *   label,serverMs,clientMs,ratio
     */
    public synchronized void exportQueryMetricsCsv(String filePath) throws IOException {
        try (FileWriter fw = new FileWriter(filePath)) {
            fw.write("label,serverMs,clientMs,ratio\n");
            for (QueryMetric qm : queryMetrics) {
                fw.write(String.format(Locale.ROOT, "%s,%.6f,%.6f,%.6f%n",
                        qm.label,
                        qm.serverMs,
                        qm.clientMs,
                        qm.ratio));
            }
        }
    }

    /**
     * Convenience alias if some older code expects this spelling.
     */
    public void exportToCsv(String filePath) throws IOException {
        exportQueryMetricsCsv(filePath);
    }

    public void exportToCSV(String filePath) throws IOException {
        exportQueryMetricsCsv(filePath);
    }

    // ========================================================================
    // Inner DTO for per-query metrics
    // ========================================================================

    public static final class QueryMetric {
        private final String label;
        private final double serverMs;
        private final double clientMs;
        private final double ratio;

        public QueryMetric(String label, double serverMs, double clientMs, double ratio) {
            this.label = (label == null) ? "" : label;
            this.serverMs = serverMs;
            this.clientMs = clientMs;
            this.ratio    = ratio;
        }

        public String getLabel()     { return label; }
        public double getServerMs()  { return serverMs; }
        public double getClientMs()  { return clientMs; }
        public double getRatio()     { return ratio; }

        @Override
        public String toString() {
            return "QueryMetric{" +
                    "label='" + label + '\'' +
                    ", serverMs=" + serverMs +
                    ", clientMs=" + clientMs +
                    ", ratio=" + ratio +
                    '}';
        }
    }
}
