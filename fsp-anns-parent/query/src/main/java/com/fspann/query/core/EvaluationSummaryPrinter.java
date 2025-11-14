package com.fspann.query.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/**
 * Consistent summary printer + CSV writer for evaluation runs.
 * - Single definition of precision@k (no micro/macro variants)
 * - Stable CSV schema for easy cross-system comparison and plotting
 */
public final class EvaluationSummaryPrinter {
    private static final Logger log = LoggerFactory.getLogger(EvaluationSummaryPrinter.class);

    // Standard K set used system-wide
    public static final List<Integer> STANDARD_KS = List.of(1, 5, 10, 20, 40, 60, 80, 100);

    public static void printAndWriteCsv(String datasetName,
                                        String profileName,
                                        int m, int lambda, int divisions,
                                        long totalIndexTimeMs,
                                        Aggregates agg,
                                        Path csvOut) {

        // -------- 1) Human-readable single line (paper-friendly) --------
        // Keep it short but information-dense.
        String precisionSummary = compactPrecision(agg.precisionAtK, STANDARD_KS);
        log.info(String.format(Locale.ROOT,
                "Avg Ratio=%.4f | P@K[%s] | Server=%.2f ms, Client=%.2f ms, ART=%.2f ms | Decrypt=%.2f ms | " +
                        "TokenB=%.1f, WorkU=%.1f | Cand[Total=%d,Keep=%d,Dec=%d,Ret=%d] | Dataset=%s | Profile=%s",
                nz(agg.avgRatio),
                precisionSummary,
                nz(agg.avgServerMs),
                nz(agg.avgClientMs),
                nz(agg.getAvgArtMs()),
                nz(agg.avgDecryptMs),
                nz(agg.avgTokenBytes),
                nz(agg.avgWorkUnits),
                Math.round(nz(agg.avgCandTotal)),
                Math.round(nz(agg.avgCandKept)),
                Math.round(nz(agg.avgCandDecrypted)),
                Math.round(nz(agg.avgReturned)),
                datasetName, profileName));

        if (agg.reencryptCount > 0 || agg.reencryptBytes > 0 || agg.reencryptMs > 0) {
            log.info(String.format(Locale.ROOT,
                    "SelectiveReencrypt: Count=%d, Bytes=%d, Time=%.2f ms",
                    agg.reencryptCount, agg.reencryptBytes, nz(agg.reencryptMs)));
        }
        if (agg.spaceMetaBytes > 0 || agg.spacePointsBytes > 0) {
            log.info(String.format(Locale.ROOT,
                    "SpaceUsage: MetaBytes=%d, PointsBytes=%d",
                    agg.spaceMetaBytes, agg.spacePointsBytes));
        }

        // -------- 2) Machine-readable CSV (stable schema) --------
        if (csvOut != null) {
            try {
                Files.createDirectories(csvOut.getParent());
                boolean exists = Files.isRegularFile(csvOut);
                if (!exists) {
                    Files.write(csvOut,
                            (csvHeader() + "\n").getBytes(StandardCharsets.UTF_8),
                            StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                }
                String row = csvRow(datasetName, profileName, m, lambda, divisions, totalIndexTimeMs, agg);
                Files.write(csvOut, (row + "\n").getBytes(StandardCharsets.UTF_8),
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (Exception e) {
                log.warn("Failed writing summary CSV at {}: {}", csvOut, e.toString());
            }
        }
    }

    /* ============================ Aggregates ============================ */

    /**
     * Immutable aggregate holder with a builder for optional fields.
     * All averages should be computed upstream (profiler/orchestrator layer),
     * and passed in here in milliseconds/bytes/units as appropriate.
     */
    public static final class Aggregates {
        // Core quality/time
        public final double avgRatio;       // your system-defined ratio (avg over queries)
        public final double avgServerMs;    // average server-side time (QueryServiceImpl.getLastQueryDurationNs)
        public final double avgClientMs;    // average client window outside server (measured at orchestrator)
        public final double avgDecryptMs;   // average decrypt time inside server (from QueryServiceImpl)

        // Candidate pipeline (averages)
        public final double avgCandTotal;
        public final double avgCandKept;
        public final double avgCandDecrypted;
        public final double avgReturned;

        // Size/complexity
        public final double avgTokenBytes;  // average estimated QueryToken size
        public final double avgWorkUnits;   // average candDecrypted * dim (empirical complexity proxy)

        // Precision per standard K
        public final Map<Integer, Double> precisionAtK;

        // Optional: selective re-encryption and space usage
        public final long   reencryptCount;
        public final long   reencryptBytes;
        public final double reencryptMs;

        public final long spaceMetaBytes;   // metadata footprint (e.g., RocksDB)
        public final long spacePointsBytes; // encrypted points footprint (if available)

        private Aggregates(Builder b) {
            this.avgRatio          = b.avgRatio;
            this.avgServerMs       = b.avgServerMs;
            this.avgClientMs       = b.avgClientMs;
            this.avgDecryptMs      = b.avgDecryptMs;

            this.avgCandTotal      = b.avgCandTotal;
            this.avgCandKept       = b.avgCandKept;
            this.avgCandDecrypted  = b.avgCandDecrypted;
            this.avgReturned       = b.avgReturned;

            this.avgTokenBytes     = b.avgTokenBytes;
            this.avgWorkUnits      = b.avgWorkUnits;

            this.precisionAtK      = Collections.unmodifiableMap(new LinkedHashMap<>(b.precisionAtK));

            this.reencryptCount    = b.reencryptCount;
            this.reencryptBytes    = b.reencryptBytes;
            this.reencryptMs       = b.reencryptMs;

            this.spaceMetaBytes    = b.spaceMetaBytes;
            this.spacePointsBytes  = b.spacePointsBytes;
        }

        /** Derived ART = server + client. */
        public double getAvgArtMs() {
            return nz(avgServerMs) + nz(avgClientMs);
        }

        public static Builder builder() { return new Builder(); }

        public static final class Builder {
            private double avgRatio = Double.NaN;
            private double avgServerMs = 0.0;
            private double avgClientMs = 0.0;
            private double avgDecryptMs = 0.0;

            private double avgCandTotal = 0.0;
            private double avgCandKept = 0.0;
            private double avgCandDecrypted = 0.0;
            private double avgReturned = 0.0;

            private double avgTokenBytes = 0.0;
            private double avgWorkUnits = 0.0;

            private final Map<Integer, Double> precisionAtK = new LinkedHashMap<>();

            private long   reencryptCount = 0L;
            private long   reencryptBytes = 0L;
            private double reencryptMs = 0.0;

            private long spaceMetaBytes = 0L;
            private long spacePointsBytes = 0L;

            public Builder avgRatio(double v) { this.avgRatio = v; return this; }
            public Builder avgServerMs(double v) { this.avgServerMs = v; return this; }
            public Builder avgClientMs(double v) { this.avgClientMs = v; return this; }
            public Builder avgDecryptMs(double v) { this.avgDecryptMs = v; return this; }

            public Builder avgCandTotal(double v) { this.avgCandTotal = v; return this; }
            public Builder avgCandKept(double v) { this.avgCandKept = v; return this; }
            public Builder avgCandDecrypted(double v) { this.avgCandDecrypted = v; return this; }
            public Builder avgReturned(double v) { this.avgReturned = v; return this; }

            public Builder avgTokenBytes(double v) { this.avgTokenBytes = v; return this; }
            public Builder avgWorkUnits(double v) { this.avgWorkUnits = v; return this; }

            /** Provide a single definition of precision@k (no micro/macro). */
            public Builder precisionAtK(Map<Integer, Double> map) {
                this.precisionAtK.clear();
                if (map != null) this.precisionAtK.putAll(map);
                return this;
            }
            public Builder precision(int k, double v) {
                this.precisionAtK.put(k, v);
                return this;
            }

            public Builder reencryptCount(long v) { this.reencryptCount = v; return this; }
            public Builder reencryptBytes(long v) { this.reencryptBytes = v; return this; }
            public Builder reencryptMs(double v) { this.reencryptMs = v; return this; }

            public Builder spaceMetaBytes(long v) { this.spaceMetaBytes = v; return this; }
            public Builder spacePointsBytes(long v) { this.spacePointsBytes = v; return this; }

            public Aggregates build() { return new Aggregates(this); }
        }
    }

    /* ============================ CSV helpers ============================ */

    private static String csvHeader() {
        // Keep schema stable across runs.
        // Precision columns follow p_at_1, p_at_5, ..., p_at_100 order.
        StringBuilder sb = new StringBuilder();
        sb.append("dataset,profile,m,lambda,divisions,index_time_ms,");
        sb.append("avg_ratio,avg_server_ms,avg_client_ms,avg_art_ms,avg_decrypt_ms,");
        sb.append("avg_token_bytes,avg_work_units,");
        sb.append("avg_cand_total,avg_cand_kept,avg_cand_decrypted,avg_returned,");
        for (int k : STANDARD_KS) sb.append("p_at_").append(k).append(",");
        sb.append("reencrypt_count,reencrypt_bytes,reencrypt_ms,");
        sb.append("space_meta_bytes,space_points_bytes");
        return sb.toString();
    }

    private static String csvRow(String dataset,
                                 String profile,
                                 int m, int lambda, int divisions,
                                 long indexTimeMs,
                                 Aggregates a) {
        StringBuilder sb = new StringBuilder();
        sb.append(escape(dataset)).append(",");
        sb.append(escape(profile)).append(",");
        sb.append(m).append(",").append(lambda).append(",").append(divisions).append(",");
        sb.append(indexTimeMs).append(",");

        // Core time/quality
        sb.append(fmt(nz(a.avgRatio))).append(",");
        sb.append(fmt(nz(a.avgServerMs))).append(",");
        sb.append(fmt(nz(a.avgClientMs))).append(",");
        sb.append(fmt(nz(a.getAvgArtMs()))).append(",");
        sb.append(fmt(nz(a.avgDecryptMs))).append(",");

        // Size/complexity
        sb.append(fmt(nz(a.avgTokenBytes))).append(",");
        sb.append(fmt(nz(a.avgWorkUnits))).append(",");

        // Candidate pipeline
        sb.append(fmt(nz(a.avgCandTotal))).append(",");
        sb.append(fmt(nz(a.avgCandKept))).append(",");
        sb.append(fmt(nz(a.avgCandDecrypted))).append(",");
        sb.append(fmt(nz(a.avgReturned))).append(",");

        // Precision@K
        for (int k : STANDARD_KS) {
            double v = a.precisionAtK.getOrDefault(k, Double.NaN);
            sb.append(fmt(v)).append(",");
        }

        // Re-encryption + Space
        sb.append(a.reencryptCount).append(",");
        sb.append(a.reencryptBytes).append(",");
        sb.append(fmt(nz(a.reencryptMs))).append(",");
        sb.append(a.spaceMetaBytes).append(",");
        sb.append(a.spacePointsBytes);

        return sb.toString();
    }

    /* ============================ small utils ============================ */

    private static String compactPrecision(Map<Integer, Double> pAtK, List<Integer> ks) {
        // Short summary like: "1:0.88,10:0.74,100:0.52"
        if (pAtK == null || pAtK.isEmpty()) return "-";
        int[] picks = new int[]{1, 10, 100};
        List<String> parts = new ArrayList<>();
        for (int k : picks) {
            Double v = pAtK.get(k);
            if (v != null && !v.isNaN()) parts.add(k + ":" + String.format(Locale.ROOT, "%.2f", v));
        }
        return parts.isEmpty() ? "-" : String.join(",", parts);
    }

    private static String escape(String s) {
        if (s == null) return "";
        if (s.contains(",") || s.contains("\"")) {
            return "\"" + s.replace("\"", "\"\"") + "\"";
        }
        return s;
    }

    private static String fmt(double v) {
        if (Double.isNaN(v) || Double.isInfinite(v)) return "";
        return String.format(Locale.ROOT, "%.6f", v);
    }

    private static double nz(Double d) {
        return (d == null || d.isNaN() || d.isInfinite()) ? 0.0 : d;
    }

    private EvaluationSummaryPrinter() {}
}
