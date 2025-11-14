package com.fspann.common;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Simple profiler for measuring durations and exporting results.
 *
 * Consistency notes:
 *  - Top-K metrics: Ratio + Precision@K (no micro/macro).
 *  - Query metrics: ServerTime (index/query), ClientWall, Overhead = max(0, Client - Server), Ratio.
 */
public class Profiler {

    // Generic duration measurements per label
    private final Map<String, Long> startTimes = new HashMap<>();
    private final Map<String, List<Long>> timings = new HashMap<>();

    // Per-query metrics (server/client/ratio) for ART-style analysis
    private final List<QueryMetric> queryMetrics = new ArrayList<>();
    private final List<Double> clientQueryTimes = new ArrayList<>();
    private final List<Double> serverQueryTimes = new ArrayList<>();
    private final List<Double> queryRatios      = new ArrayList<>();

    // Simple Top-K rows (Ratio + Precision@K)
    private final List<String[]> topKRecords = new ArrayList<>();

    /* ---------------------------------------------------------
     * Generic timing API
     * --------------------------------------------------------- */

    public void start(String label) {
        startTimes.put(label, System.nanoTime());
    }

    public void stop(String label) {
        long end   = System.nanoTime();
        long start = startTimes.getOrDefault(label, end);
        long duration = end - start;
        timings.computeIfAbsent(label, k -> new ArrayList<>()).add(duration);
    }

    public void log(String label) {
        List<Long> all = timings.get(label);
        if (all == null || all.isEmpty()) return;
        double avgMs = all.stream().mapToLong(Long::longValue).average().orElse(0.0) / 1_000_000.0;
        System.out.printf("⏱️ %s: avg %.2f ms (over %d runs)%n", label, avgMs, all.size());
    }

    public void logMemory(String label) {
        Runtime runtime = Runtime.getRuntime();
        long used = runtime.totalMemory() - runtime.freeMemory();
        System.out.printf("📦 %s: %.2f MB RAM%n", label, used / 1024.0 / 1024.0);
    }

    public void exportToCSV(String filePath) {
        try (FileWriter fw = new FileWriter(filePath)) {
            fw.write("Label,AvgTime(ms),Runs\n");
            for (Map.Entry<String, List<Long>> e : timings.entrySet()) {
                double avgMs = e.getValue().stream().mapToLong(Long::longValue).average().orElse(0.0) / 1_000_000.0;
                fw.write(String.format("%s,%.4f,%d%n", e.getKey(), avgMs, e.getValue().size()));
            }
            System.out.println("Profiler data written to " + filePath);
        } catch (IOException ex) {
            System.err.println("Failed to write profiler CSV: " + ex.getMessage());
        }
    }

    public List<Long> getTimings(String label) {
        return timings.getOrDefault(label, Collections.emptyList());
    }

    /* ---------------------------------------------------------
     * Top-K variants: Ratio + Precision@K
     * --------------------------------------------------------- */

    /**
     * Record one Top-K variant for a given query.
     *
     * @param queryId  logical query identifier
     * @param topK     requested K
     * @param retrieved number of results returned for this K
     * @param ratio    custom ratio metric (your ANN ratio)
     * @param recall   (now used as) precision@K (for compatibility; semantic = precision)
     * @param timeMs   time in ms (typically server index/query time or run time depending on caller)
     */
    public void recordTopKVariants(String queryId,
                                   int topK,
                                   int retrieved,
                                   double ratio,
                                   double recall,
                                   long timeMs) {

        // Interpret 'recall' parameter as the unified Precision@K metric.
        double precision = recall;

        topKRecords.add(new String[]{
                queryId,
                String.valueOf(topK),
                String.valueOf(retrieved),
                String.format(Locale.ROOT, "%.4f", ratio),
                String.format(Locale.ROOT, "%.4f", precision),
                String.valueOf(timeMs)
        });
    }

    public void exportTopKVariants(String filePath) {
        try (FileWriter fw = new FileWriter(filePath)) {
            // Column name 'Precision' instead of 'Recall' to match system-wide semantics
            fw.write("QueryID,TopK,Retrieved,Ratio,Precision,TimeMs\n");
            for (String[] row : topKRecords) {
                fw.write(String.join(",", row));
                fw.write("\n");
            }
            System.out.println("📤 Top-K evaluation written to " + filePath);
        } catch (IOException ex) {
            System.err.println("Failed to write Top-K evaluation CSV: " + ex.getMessage());
        }
    }

    /* ---------------------------------------------------------
     * Per-query ART metrics (server/client/ratio)
     * --------------------------------------------------------- */

    /**
     * Record a single query's timing and ratio.
     *
     * @param label    query id/label (e.g., "Q0", "query_123")
     * @param serverMs server-side time (index/query; typically from QueryServiceImpl)
     * @param clientMs client-side measured wall time for the same query
     * @param ratio    custom ratio metric
     */
    public void recordQueryMetric(String label, double serverMs, double clientMs, double ratio) {
        QueryMetric qm = new QueryMetric(label, serverMs, clientMs, ratio);
        queryMetrics.add(qm);

        // Maintain legacy lists for any existing consumers
        serverQueryTimes.add(serverMs);
        clientQueryTimes.add(clientMs);
        queryRatios.add(ratio);
    }

    /**
     * Export per-query metrics in a consistent ART schema:
     *  QueryId,ServerQueryMs,ClientEndToEndMs,OverheadMs,Ratio
     */
    public void exportQueryMetrics(String filePath) {
        try (FileWriter fw = new FileWriter(filePath)) {
            fw.write("QueryId,ServerQueryMs,ClientEndToEndMs,OverheadMs,Ratio\n");

            if (!queryMetrics.isEmpty()) {
                for (QueryMetric qm : queryMetrics) {
                    double s = qm.serverMs;
                    double c = qm.clientMs;
                    double overhead = Math.max(0.0, c - s);
                    fw.write(String.format(Locale.ROOT, "%s,%.4f,%.4f,%.4f,%.4f%n",
                            qm.id, s, c, overhead, qm.ratio));
                }
            } else {
                // Fallback to legacy lists if queryMetrics wasn't used for some reason
                for (int i = 0; i < clientQueryTimes.size(); i++) {
                    double s = serverQueryTimes.get(i);
                    double c = clientQueryTimes.get(i);
                    double overhead = Math.max(0.0, c - s);
                    fw.write(String.format(Locale.ROOT, "Q%d,%.4f,%.4f,%.4f,%.4f%n",
                            i + 1, s, c, overhead, queryRatios.get(i)));
                }
            }

            System.out.println("📤 Query metrics written to " + filePath);
        } catch (IOException ex) {
            System.err.println("Failed to write query metrics CSV: " + ex.getMessage());
        }
    }

    /* ---------------------------------------------------------
     * Accessors (for post-hoc analysis)
     * --------------------------------------------------------- */

    public List<Double> getAllClientQueryTimes() {
        return clientQueryTimes;
    }

    public List<Double> getAllServerQueryTimes() {
        return serverQueryTimes;
    }

    public List<Double> getAllQueryRatios() {
        return queryRatios;
    }

    /* ---------------------------------------------------------
     * Internal types
     * --------------------------------------------------------- */

    private static class QueryMetric {
        final String id;
        final double serverMs;
        final double clientMs;
        final double ratio;

        QueryMetric(String id, double serverMs, double clientMs, double ratio) {
            this.id = id;
            this.serverMs = serverMs;
            this.clientMs = clientMs;
            this.ratio = ratio;
        }
    }
}
