package com.fspann.query.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

public final class EvaluationSummaryPrinter {

    private static final Logger log = LoggerFactory.getLogger(EvaluationSummaryPrinter.class);

    public static final List<Integer> STANDARD_KS = List.of(20, 40, 60, 80, 100);

    public static void printAndWriteCsv(
            String datasetName,
            String profileName,
            int m,
            int lambda,
            int divisions,
            long totalIndexTimeMs,
            Aggregates agg,
            Path summaryCsvPath
    ) {
        Objects.requireNonNull(agg);

        // HUMAN READABLE PAPER-LINE
        String recallLine = compactRecall(agg.recallAtK, STANDARD_KS);
        String distRatioLine = compactRecall(agg.distanceRatioAtK, STANDARD_KS);

        log.info(String.format(Locale.ROOT,
                "DistanceRatio=%.4f | CandidateRatio=%.4f | Recall@K[%s] | DistRatio@K[%s] | " +
                        "Server=%.2f ms | Client=%.2f ms | ART=%.2f ms | Decrypt=%.2f ms | " +
                        "TokenB=%.1f | WorkU=%.1f | Cand{T=%d,K=%d,D=%d,R=%d} | DS=%s | Profile=%s",
                nz(agg.avgDistanceRatio),
                nz(agg.avgCandidateRatio),
                recallLine,
                distRatioLine,
                nz(agg.avgServerMs),
                nz(agg.avgClientMs),
                nz(agg.getAvgArtMs()),
                nz(agg.avgDecryptMs),
                nz(agg.avgTokenBytes),
                nz(agg.avgWorkUnits),
                round(agg.avgCandTotal),
                round(agg.avgCandKept),
                round(agg.avgCandDecrypted),
                round(agg.avgReturned),
                datasetName,
                profileName
        ));

        if (agg.reencryptCount > 0 || agg.reencryptBytes > 0 || agg.reencryptMs > 0) {
            log.info(String.format(Locale.ROOT,
                    "ReEncrypt: Count=%d, Bytes=%d, Time=%.2f ms",
                    agg.reencryptCount, agg.reencryptBytes, nz(agg.reencryptMs)));
        }
        if (agg.spaceMetaBytes > 0 || agg.spacePointsBytes > 0) {
            log.info(String.format(Locale.ROOT,
                    "Space: Meta=%d bytes, Points=%d bytes",
                    agg.spaceMetaBytes, agg.spacePointsBytes));
        }

        if (summaryCsvPath != null) {
            writeSummaryCsv(datasetName, profileName, m, lambda, divisions,
                    totalIndexTimeMs, agg, summaryCsvPath);
        }

        Path accCsv = summaryCsvPath.resolveSibling("accuracy.csv");
        writeAccuracyCsv(datasetName, profileName, m, lambda, divisions,
                totalIndexTimeMs, agg, accCsv);

        Path costCsv = summaryCsvPath.resolveSibling("cost.csv");
        writeCostCsv(datasetName, profileName, m, lambda, divisions,
                totalIndexTimeMs, agg, costCsv);
    }

    // --- SUMMARY.CSV (UPDATED: both ratios) ---

    private static void writeSummaryCsv(
            String dataset,
            String profile,
            int m,
            int lambda,
            int divisions,
            long indexTimeMs,
            Aggregates a,
            Path csvOut
    ) {
        try {
            Files.createDirectories(csvOut.getParent());
            boolean exists = Files.isRegularFile(csvOut);
            if (!exists) {
                Files.write(csvOut,
                        (csvHeader() + "\n").getBytes(StandardCharsets.UTF_8),
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            }
            String row = csvRow(dataset, profile, m, lambda, divisions, indexTimeMs, a);
            Files.write(csvOut, (row + "\n").getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (Exception e) {
            log.warn("Failed to write summary CSV {}: {}", csvOut, e.toString());
        }
    }

    // --- ACCURACY.CSV (UPDATED: both ratios) ---

    private static void writeAccuracyCsv(
            String dataset,
            String profile,
            int m,
            int lambda,
            int divisions,
            long indexMs,
            Aggregates a,
            Path out
    ) {
        try {
            Files.createDirectories(out.getParent());
            boolean exists = Files.isRegularFile(out);

            if (!exists) {
                StringBuilder h = new StringBuilder();
                h.append("dataset,profile,m,lambda,divisions,index_time_ms,");
                h.append("avg_distance_ratio,avg_candidate_ratio,avg_recall,");
                h.append("avg_server_ms,avg_client_ms,avg_art_ms,avg_decrypt_ms,");
                for (int k : STANDARD_KS) h.append("recall_at_").append(k).append(",");
                for (int k : STANDARD_KS) h.append("distance_ratio_at_").append(k).append(",");
                String header = h.substring(0, h.length() - 1);
                Files.write(out, (header + "\n").getBytes(StandardCharsets.UTF_8),
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            }

            StringBuilder sb = new StringBuilder();
            sb.append(escape(dataset)).append(",");
            sb.append(escape(profile)).append(",");
            sb.append(m).append(",");
            sb.append(lambda).append(",");
            sb.append(divisions).append(",");
            sb.append(indexMs).append(",");

            sb.append(fmt(nz(a.avgDistanceRatio))).append(",");
            sb.append(fmt(nz(a.avgCandidateRatio))).append(",");
            sb.append(fmt(nz(a.avgRecall))).append(",");

            sb.append(fmt(nz(a.avgServerMs))).append(",");
            sb.append(fmt(nz(a.avgClientMs))).append(",");
            sb.append(fmt(nz(a.getAvgArtMs()))).append(",");
            sb.append(fmt(nz(a.avgDecryptMs))).append(",");

            for (int k : STANDARD_KS) {
                sb.append(fmt(a.recallAtK.getOrDefault(k, Double.NaN))).append(",");
            }

            for (int k : STANDARD_KS) {
                sb.append(fmt(a.distanceRatioAtK.getOrDefault(k, Double.NaN))).append(",");
            }

            String row = sb.substring(0, sb.length() - 1);
            Files.write(out, (row + "\n").getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);

        } catch (Exception ex) {
            log.warn("Failed to write accuracy CSV {}: {}", out, ex.toString());
        }
    }

    // --- COST.CSV (unchanged) ---

    private static void writeCostCsv(
            String dataset,
            String profile,
            int m,
            int lambda,
            int divisions,
            long indexMs,
            Aggregates a,
            Path out
    ) {
        try {
            Files.createDirectories(out.getParent());
            boolean exists = Files.isRegularFile(out);

            if (!exists) {
                String header =
                        "dataset,profile,m,lambda,divisions,index_time_ms," +
                                "avg_token_bytes,avg_work_units," +
                                "avg_cand_total,avg_cand_kept,avg_cand_decrypted,avg_returned," +
                                "reencrypt_count,reencrypt_bytes,reencrypt_ms," +
                                "space_meta_bytes,space_points_bytes";
                Files.write(out, (header + "\n").getBytes(StandardCharsets.UTF_8),
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            }

            StringBuilder sb = new StringBuilder();
            sb.append(escape(dataset)).append(",");
            sb.append(escape(profile)).append(",");
            sb.append(m).append(",").append(lambda).append(",").append(divisions).append(",");
            sb.append(indexMs).append(",");
            sb.append(fmt(nz(a.avgTokenBytes))).append(",");
            sb.append(fmt(nz(a.avgWorkUnits))).append(",");
            sb.append(fmt(nz(a.avgCandTotal))).append(",");
            sb.append(fmt(nz(a.avgCandKept))).append(",");
            sb.append(fmt(nz(a.avgCandDecrypted))).append(",");
            sb.append(fmt(nz(a.avgReturned))).append(",");
            sb.append(a.reencryptCount).append(",");
            sb.append(a.reencryptBytes).append(",");
            sb.append(fmt(nz(a.reencryptMs))).append(",");
            sb.append(a.spaceMetaBytes).append(",");
            sb.append(a.spacePointsBytes);

            Files.write(out, (sb + "\n").getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);

        } catch (Exception ex) {
            log.warn("Failed to write cost CSV {}: {}", out, ex.toString());
        }
    }

    // --- CSV HEADER (UPDATED: both ratios) ---

    private static String csvHeader() {
        StringBuilder sb = new StringBuilder();
        sb.append("dataset,profile,m,lambda,divisions,index_time_ms,");
        sb.append("avg_distance_ratio,avg_candidate_ratio,avg_recall,");
        sb.append("avg_server_ms,avg_client_ms,avg_art_ms,avg_decrypt_ms,");
        sb.append("avg_token_bytes,avg_work_units,");
        sb.append("avg_cand_total,avg_cand_kept,avg_cand_decrypted,avg_returned,");
        for (int k : STANDARD_KS) sb.append("recall_at_").append(k).append(",");
        for (int k : STANDARD_KS) sb.append("distance_ratio_at_").append(k).append(",");
        sb.append("reencrypt_count,reencrypt_bytes,reencrypt_ms,");
        sb.append("space_meta_bytes,space_points_bytes");
        return sb.toString();
    }

    private static String csvRow(
            String dataset,
            String profile,
            int m,
            int lambda,
            int divisions,
            long indexTimeMs,
            Aggregates a
    ) {
        StringBuilder sb = new StringBuilder();
        sb.append(escape(dataset)).append(",");
        sb.append(escape(profile)).append(",");
        sb.append(m).append(",").append(lambda).append(",").append(divisions).append(",");
        sb.append(indexTimeMs).append(",");

        sb.append(fmt(nz(a.avgDistanceRatio))).append(",");
        sb.append(fmt(nz(a.avgCandidateRatio))).append(",");
        sb.append(fmt(nz(a.avgRecall))).append(",");

        sb.append(fmt(nz(a.avgServerMs))).append(",");
        sb.append(fmt(nz(a.avgClientMs))).append(",");
        sb.append(fmt(nz(a.getAvgArtMs()))).append(",");
        sb.append(fmt(nz(a.avgDecryptMs))).append(",");

        sb.append(fmt(nz(a.avgTokenBytes))).append(",");
        sb.append(fmt(nz(a.avgWorkUnits))).append(",");

        sb.append(fmt(nz(a.avgCandTotal))).append(",");
        sb.append(fmt(nz(a.avgCandKept))).append(",");
        sb.append(fmt(nz(a.avgCandDecrypted))).append(",");
        sb.append(fmt(nz(a.avgReturned))).append(",");

        for (int k : STANDARD_KS) {
            sb.append(fmt(a.recallAtK.getOrDefault(k, Double.NaN))).append(",");
        }

        for (int k : STANDARD_KS) {
            sb.append(fmt(a.distanceRatioAtK.getOrDefault(k, Double.NaN))).append(",");
        }

        sb.append(a.reencryptCount).append(",");
        sb.append(a.reencryptBytes).append(",");
        sb.append(fmt(nz(a.reencryptMs))).append(",");
        sb.append(a.spaceMetaBytes).append(",");
        sb.append(a.spacePointsBytes);

        return sb.toString();
    }

    // --- UTILS (unchanged) ---

    private static int round(double v) { return (int) Math.round(v); }

    private static String compactRecall(Map<Integer, Double> rAtK, List<Integer> ks) {
        if (rAtK == null || rAtK.isEmpty()) return "-";
        ArrayList<String> parts = new ArrayList<>();
        for (int k : ks) {
            Double v = rAtK.get(k);
            if (v != null && !v.isNaN())
                parts.add(k + ":" + String.format(Locale.ROOT, "%.2f", v));
        }
        return parts.isEmpty() ? "-" : String.join(",", parts);
    }

    private static String escape(String s) {
        if (s == null) return "";
        if (s.contains(",") || s.contains("\""))
            return "\"" + s.replace("\"", "\"\"") + "\"";
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