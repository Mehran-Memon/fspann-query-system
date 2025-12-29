package com.fspann.common;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Profiler
 *
 * Responsibilities:
 *  - Store per-label timings (ns)
 *  - Store per-query metrics needed by Aggregates
 *  - Backwards compatible with previous Profiler API
 *
 * NOTE:
 *  Aggregation is NOT done here. This class only stores
 *  atomic observations. ForwardSecureANNSystem performs
 *  aggregation and sends Aggregates → EvaluationSummaryPrinter.
 */
public final class Profiler {

    /* -----------------------------------------------------
     * Timing buckets: generic label → list<durationNs>
     * ----------------------------------------------------- */
    private final Map<String, Long> startTimes = new HashMap<>();
    private final Map<String, List<Long>> timings = new LinkedHashMap<>();

    /* -----------------------------------------------------
     * Per-query unified metrics (one row per query)
     * ----------------------------------------------------- */
    private final List<QueryRow> queryRows = new ArrayList<>();

    /* Optional: arbitrary multi-column tables (e.g., debug) */
    private final List<String[]> topKRecords = new ArrayList<>();

    /* -----------------------------------------------------
     * TIMING API
     * ----------------------------------------------------- */

    public synchronized void start(String label) {
        startTimes.put(label, System.nanoTime());
    }

    public synchronized void stop(String label) {
        Long st = startTimes.remove(label);
        if (st == null) return;
        long dt = Math.max(0, System.nanoTime() - st);
        timings.computeIfAbsent(label, x -> new ArrayList<>()).add(dt);
    }

    public synchronized void recordTiming(String label, long durationNs) {
        if (durationNs < 0) durationNs = 0;
        timings.computeIfAbsent(label, x -> new ArrayList<>()).add(durationNs);
    }

    public synchronized Map<String, List<Long>> getTimings() {
        Map<String, List<Long>> cp = new LinkedHashMap<>();
        for (var e : timings.entrySet()) {
            cp.put(e.getKey(), new ArrayList<>(e.getValue()));
        }
        return cp;
    }

    public synchronized List<Long> getTimings(String label) {
        List<Long> v = timings.get(label);
        return (v == null) ? Collections.emptyList() : new ArrayList<>(v);
    }

    /* -----------------------------------------------------
     * RESET
     * ----------------------------------------------------- */

    public synchronized void reset() {
        startTimes.clear();
        timings.clear();
        queryRows.clear();
        topKRecords.clear();
    }

    /* -----------------------------------------------------
     * PER-QUERY METRIC API  (Option-C)
     * ----------------------------------------------------- */

    /**
     * Store all unified query metrics in one structured row.
     * ForwardSecureANNSystem constructs QueryEvaluationResult
     * and then calls this method to persist it.
     */
    public synchronized void recordQueryRow(
            String label,
            double serverMs,
            double clientMs,
            double runMs,
            double decryptMs,
            double insertMs,
            double ratio,
            double precision,
            int candTotal,
            int candKept,
            int candDecrypted,
            int candReturned,
            int tokenBytes,
            int vectorDim,
            int tokenK,
            int tokenKBase,
            int qIndex,
            int totalFlushed,
            int flushThreshold,
            int touchedCount,
            int reencCount,
            long reencTimeMs,
            long reencBytesDelta,
            long reencBytesAfter,
            String ratioDenomSource,
            String mode,
            int stableRaw,
            int stableFinal,
            int nnRank,
            boolean nnSeen
    ) {
        queryRows.add(new QueryRow(
                label,
                serverMs,
                clientMs,
                runMs,
                decryptMs,
                insertMs,
                ratio,
                precision,
                candTotal,
                candKept,
                candDecrypted,
                candReturned,
                tokenBytes,
                vectorDim,
                tokenK,
                tokenKBase,
                qIndex,
                totalFlushed,
                flushThreshold,
                touchedCount,
                reencCount,
                reencTimeMs,
                reencBytesDelta,
                reencBytesAfter,
                ratioDenomSource,
                mode,
                stableRaw,
                stableFinal,
                nnRank,
                nnSeen
        ));
    }


    /** Defensive copy of stored rows. */
    public synchronized List<QueryRow> getQueryRows() {
        return new ArrayList<>(queryRows);
    }

    /* -----------------------------------------------------
     * Top-K debug rows
     * ----------------------------------------------------- */

    public synchronized void addTopKRecord(String... cols) {
        if (cols != null) topKRecords.add(Arrays.copyOf(cols, cols.length));
    }

    public synchronized List<String[]> getTopKRecords() {
        List<String[]> out = new ArrayList<>(topKRecords.size());
        for (String[] r : topKRecords) out.add(Arrays.copyOf(r, r.length));
        return out;
    }

    /* -----------------------------------------------------
     * OPTIONAL CSV export (legacy compatibility)
     * Still only exports basic trio (server/client/ratio),
     * full CSV produced by EvaluationSummaryPrinter instead.
     * ----------------------------------------------------- */

    public synchronized void exportQueryMetricsCsv(String fp) throws IOException {
        try (FileWriter fw = new FileWriter(fp)) {
            fw.write("label,serverMs,clientMs,ratio\n");
            for (QueryRow qr : queryRows) {
                fw.write(String.format(Locale.ROOT,
                        "%s,%.6f,%.6f,%.6f%n",
                        qr.label,
                        qr.serverMs,
                        qr.clientMs,
                        qr.ratio));
            }
        }
    }

    public void exportToCSV(String fp) throws IOException { exportQueryMetricsCsv(fp); }

    /* -----------------------------------------------------
     * QUERY ROW DTO – unify with QueryEvaluationResult
     * ----------------------------------------------------- */
    public static final class QueryRow {
        public final String label;

        public final double serverMs;
        public final double clientMs;
        public final double runMs;
        public final double decryptMs;
        public final double insertMs;

        public final double ratio;
        public final double precision;

        public final int candTotal;
        public final int candKept;
        public final int candDecrypted;
        public final int candReturned;

        public final int tokenBytes;
        public final int vectorDim;
        public final int tokenK;
        public final int tokenKBase;
        public final int qIndex;

        public final int totalFlushed;
        public final int flushThreshold;

        public final int touchedCount;
        public final int reencCount;
        public final long reencTimeMs;
        public final long reencBytesDelta;
        public final long reencBytesAfter;

        public final String ratioDenomSource;
        public final String mode;

        public final int stableRaw;
        public final int stableFinal;

        public final int nnRank;
        public final boolean nnSeen;

        private QueryRow(
                String label,
                double serverMs,
                double clientMs,
                double runMs,
                double decryptMs,
                double insertMs,
                double ratio,
                double precision,
                int candTotal,
                int candKept,
                int candDecrypted,
                int candReturned,
                int tokenBytes,
                int vectorDim,
                int tokenK,
                int tokenKBase,
                int qIndex,
                int totalFlushed,
                int flushThreshold,
                int touched,
                int reencCount,
                long reencMs,
                long reencDelta,
                long reencAfter,
                String denom,
                String mode,
                int stableRaw,
                int stableFinal,
                int nnRank,
                Boolean nnSeen
        ) {
            this.label = label;

            this.serverMs = serverMs;
            this.clientMs = clientMs;
            this.runMs = runMs;
            this.decryptMs = decryptMs;
            this.insertMs = insertMs;

            this.ratio = ratio;
            this.precision = precision;

            this.candTotal = candTotal;
            this.candKept = candKept;
            this.candDecrypted = candDecrypted;
            this.candReturned = candReturned;

            this.tokenBytes = tokenBytes;
            this.vectorDim = vectorDim;
            this.tokenK = tokenK;
            this.tokenKBase = tokenKBase;
            this.qIndex = qIndex;

            this.totalFlushed = totalFlushed;
            this.flushThreshold = flushThreshold;

            this.touchedCount = touched;
            this.reencCount = reencCount;
            this.reencTimeMs = reencMs;
            this.reencBytesDelta = reencDelta;
            this.reencBytesAfter = reencAfter;

            this.ratioDenomSource = denom;
            this.mode = mode;

            this.stableRaw = stableRaw;
            this.stableFinal = stableFinal;

            this.nnRank = nnRank;
            this.nnSeen = nnSeen;
        }
    }
}
