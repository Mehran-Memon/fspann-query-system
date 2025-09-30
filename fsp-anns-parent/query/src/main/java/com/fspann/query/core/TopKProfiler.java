package com.fspann.query.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class TopKProfiler {
    private static final Logger logger = LoggerFactory.getLogger(TopKProfiler.class);
    private final List<String[]> rows = new ArrayList<>();
    private final String baseDir;
    // Used to compute scanned fractions
    private volatile long datasetSize = -1L;

    public TopKProfiler(String baseDir) {
        this.baseDir = Objects.requireNonNull(baseDir, "Base directory cannot be null");
    }

    private static String csvEscape(String s) {
        if (s == null) return "";
        boolean needsQuotes = s.indexOf(',') >= 0 || s.indexOf('"') >= 0 || s.indexOf('\n') >= 0 || s.indexOf('\r') >= 0;
        if (!needsQuotes) return s;
        String escaped = s.replace("\"", "\"\"");
        return "\"" + escaped + "\"";
    }

    /** Allow exporter to compute SF = candidates/N, SF_Touched = touched/N, SF_Reenc = reenc/N. */
    public void setDatasetSize(long n) { this.datasetSize = n; }

    public void record(String queryId,
                       List<QueryEvaluationResult> results,
                       int candTotal, int candKeptVersion, int candDecrypted, int returnedForBaseSearch) {
        Objects.requireNonNull(queryId, "Query ID cannot be null");
        Objects.requireNonNull(results, "Results cannot be null");

        String qid = csvEscape(queryId);

        synchronized (rows) {
            for (QueryEvaluationResult r : results) {
                rows.add(new String[]{
                        qid,                                     // 0
                        String.valueOf(r.getTopKRequested()),    // 1
                        String.valueOf(r.getRetrieved()),        // 2
                        (Double.isNaN(r.getRatio())     ? "NaN" : String.format(Locale.ROOT, "%.4f", r.getRatio())),      // 3
                        (Double.isNaN(r.getPrecision()) ? "NaN" : String.format(Locale.ROOT, "%.4f", r.getPrecision())),  // 4
                        String.valueOf(r.getTimeMs()),           // 5  ServerTimeMs
                        String.valueOf(r.getClientTimeMs()),     // 6  ClientTimeMs
                        String.valueOf(r.getInsertTimeMs()),     // 7
                        String.valueOf(r.getCandidateCount()),   // 8
                        String.valueOf(r.getTokenSizeBytes()),   // 9
                        String.valueOf(r.getVectorDim()),        // 10
                        String.valueOf(candTotal),               // 11
                        String.valueOf(candKeptVersion),         // 12
                        String.valueOf(candDecrypted),           // 13
                        String.valueOf(r.getRetrieved()),        // 14  Returned (same as retrieved for that k)
                        String.valueOf(r.getTouchedCount()),     // 15
                        String.valueOf(r.getReencryptedCount()), // 16
                        String.valueOf(r.getReencTimeMs()),      // 17
                        String.valueOf(r.getReencBytesDelta()),  // 18
                        String.valueOf(r.getReencBytesAfter()),  // 19
                        // NEW clarifiers
                        (r.getRatioDenomSource() == null ? "none" : r.getRatioDenomSource()),  // 20
                        String.valueOf(r.getTokenK()),           // 21
                        String.valueOf(r.getTokenKBase()),       // 22
                        String.valueOf(r.getQIndexZeroBased()),  // 23
                        (r.getCandMetricsMode() == null ? "full" : r.getCandMetricsMode())      // 24
                });
            }
        }
    }

    public void export(String filePath) {
        Objects.requireNonNull(filePath, "File path cannot be null");

        Path basePath = Paths.get(baseDir).normalize().toAbsolutePath();
        Path outPath  = Paths.get(filePath);
        if (!outPath.isAbsolute()) outPath = basePath.resolve(outPath);
        outPath = outPath.normalize().toAbsolutePath();

        if (!outPath.startsWith(basePath)) {
            logger.error("Export path {} is outside profiler baseDir {}", outPath, basePath);
            throw new IllegalArgumentException("Invalid export path: " + filePath);
        }
        final List<String[]> snapshot;
        synchronized (rows) {
            snapshot = new ArrayList<>(rows);
        }

        try {
            Files.createDirectories(outPath.getParent());
            try (BufferedWriter bw = Files.newBufferedWriter(outPath)) {
                bw.write(
                        "QueryID,TopK,Retrieved,Ratio,Precision,ServerTimeMs,ClientTimeMs,InsertTimeMs," +
                                "CandidateCount,TokenSizeBytes,VectorDim," +
                                "CandTotal,CandKeptVersion,CandDecrypted,Returned," +
                                "TouchedCount,ReencCount,ReencTimeMs,ReencDeltaBytes,ReencAfterBytes," +
                                "RatioDenomSource,TokenK,TokenKBase,qIndexZeroBased,cand_metrics_mode," +
                                "CF_req,CF_ret,SF,SF_Touched,SF_Reenc\n"
                );

                for (String[] row : snapshot) {
                    int candTotalEval = parseIntSafe(row[11], 0);  // CandTotal
                    int topK       = parseIntSafe(row[1], 0);
                    int retrieved  = parseIntSafe(row[2], 0);
                    int touchedCnt = parseIntSafe(row[15], 0);
                    int reencCnt   = parseIntSafe(row[16], 0);

                    String cfReq = (topK > 0 && candTotalEval >= 0)
                            ? fmtDouble((double) candTotalEval / (double) topK) : "NaN";
                    String cfRet = (retrieved > 0 && candTotalEval >= 0)
                            ? fmtDouble((double) candTotalEval / (double) retrieved) : "NaN";

                    String sfEval    = (datasetSize > 0 && candTotalEval >= 0)
                            ? fmtDouble((double) candTotalEval / (double) datasetSize) : "NaN";
                    String sfTouched = (datasetSize > 0 && touchedCnt >= 0)
                            ? fmtDouble((double) touchedCnt / (double) datasetSize) : "NaN";
                    String sfReenc   = (datasetSize > 0 && reencCnt >= 0)
                            ? fmtDouble((double) reencCnt / (double) datasetSize) : "NaN";

                    // Write recorded columns
                    bw.write(String.join(",", row));
                    // Append derived columns
                    bw.write(","); bw.write(cfReq);
                    bw.write(","); bw.write(cfRet);
                    bw.write(","); bw.write(sfEval);
                    bw.write(","); bw.write(sfTouched);
                    bw.write(","); bw.write(sfReenc);
                    bw.write("\n");
                }
            }
            logger.info("Top-K evaluation written to {}", outPath);
        } catch (IOException ex) {
            logger.error("Failed to write top-K evaluation CSV to {}", outPath, ex);
            throw new RuntimeException("Failed to write CSV: " + outPath, ex);
        }
    }

    private static String safeText(String s, String def) {
        if (s == null || s.isBlank()) return def;
        return s;
    }
    private static String rowAt(String[] row, int idx) {
        return (idx < row.length) ? row[idx] : "";
    }
    private static String fmtDouble(double v) { return String.format(Locale.ROOT, "%.6f", v); }
    private static int parseIntSafe(String s, int def) {
        try { return Integer.parseInt(s); } catch (Exception ignore) { return def; }
    }
    /** Clear accumulated rows (e.g., between runs). */
    public void reset() {
        synchronized (rows) { rows.clear(); }
    }
    /** Current number of recorded rows. */
    public int size() {
        synchronized (rows) { return rows.size(); }
    }

}
