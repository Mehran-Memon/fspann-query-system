package com.fspann.query.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Locale;

public final class EvaluationSummaryPrinter {
    private static final Logger log = LoggerFactory.getLogger(EvaluationSummaryPrinter.class);

    public static void printAndWriteCsv(String datasetName,
                                        String profileName,
                                        int m, int lambda, int divisions,
                                        long totalIndexTimeMs,
                                        Aggregates agg,                // see builder below
                                        Path csvOut) {
        // 1) Emit the single, unambiguous console line you asked for
        log.info(String.format(Locale.ROOT,
                "Average Ratio: %.4f | Average Run Time: %.2f ms (Query=%.2f ms, Client=%.2f ms) | Dataset=%s | Profile=%s",
                agg.avgRatio, agg.avgRunMs, agg.avgQueryMs, agg.avgClientMs, datasetName, profileName));

        // 2) Persist machine-readable row (append)
        if (csvOut != null) {
            try {
                Files.createDirectories(csvOut.getParent());
                boolean exists = Files.isRegularFile(csvOut);
                if (!exists) {
                    Files.write(csvOut,
                            "dataset,profile,m,lambda,divisions,avg_ratio,avg_query_ms,avg_client_ms,avg_run_ms,index_time_ms\n"
                                    .getBytes(StandardCharsets.UTF_8),
                            StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                }
                String row = String.format(Locale.ROOT,
                        "%s,%s,%d,%d,%d,%.6f,%.2f,%.2f,%.2f,%d%n",
                        datasetName, profileName, m, lambda, divisions,
                        agg.avgRatio, agg.avgQueryMs, agg.avgClientMs, agg.avgRunMs, totalIndexTimeMs);
                Files.write(csvOut, row.getBytes(StandardCharsets.UTF_8),
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (Exception e) {
                log.warn("Failed writing summary CSV at {}: {}", csvOut, e.toString());
            }
        }
    }

    /** Simple aggregate holder (plug your profiler means here). */
    public static final class Aggregates {
        public final double avgRatio;
        public final double avgQueryMs;
        public final double avgClientMs;
        public final double avgRunMs;

        public Aggregates(double avgRatio, double avgQueryMs, double avgClientMs) {
            this.avgRatio = avgRatio;
            this.avgQueryMs = avgQueryMs;
            this.avgClientMs = Math.max(0.0, avgClientMs);
            this.avgRunMs = this.avgQueryMs + this.avgClientMs;
        }
    }

    private EvaluationSummaryPrinter() {}
}
