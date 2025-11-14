package com.fspann.query.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Consistent Top-K profiler:
 *  • Single precision metric (Precision@K)
 *  • Server / Client / Run / Decrypt time split
 *  • Candidate pipeline: candTotal, candKept, candDecrypted, candReturned
 *  • Re-encryption stats + derived factors (CF_req, CF_ret, SF_*)
 */
public class TopKProfiler {
    private static final Logger logger = LoggerFactory.getLogger(TopKProfiler.class);

    private final List<String[]> rows = new ArrayList<>();
    private final String baseDir;
    /** Used to compute scanning fractions: SF = candidates / N, etc. */
    private volatile long datasetSize = -1L;

    public TopKProfiler(String baseDir) {
        this.baseDir = Objects.requireNonNull(baseDir, "Base directory cannot be null");
    }

    /** Allow exporter to compute SF = candidates/N, SF_Touched = touched/N, SF_Reenc = reenc/N. */
    public void setDatasetSize(long n) { this.datasetSize = n; }

    // -------------------------------------------------------------------------
    // Recording (modern)
    // -------------------------------------------------------------------------

    /** Preferred API: take metrics directly from QueryEvaluationResult. */
    public void record(String queryId, List<QueryEvaluationResult> results) {
        recordInternal(queryId, results, null, null, null, null);
    }

    /** Legacy-compatible overload with optional overrides. */
    public void record(String queryId,
                       List<QueryEvaluationResult> results,
                       int candTotalOverride,
                       int candKeptOverride,
                       int candDecryptedOverride,
                       int returnedForBaseSearchOverride) {
        recordInternal(queryId, results,
                candTotalOverride, candKeptOverride, candDecryptedOverride, returnedForBaseSearchOverride);
    }

    // -------------------------------------------------------------------------
    // Export
    // -------------------------------------------------------------------------

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
                // Stable unified header
                bw.write(
                        "QueryID,TopK,Returned,Ratio,Precision," +
                                "ServerTimeMs,ClientTimeMs,RunTimeMs,DecryptTimeMs,InsertTimeMs," +
                                "CandTotal,CandKept,CandDecrypted,CandReturned," +
                                "TokenSizeBytes,VectorDim," +
                                "TouchedCount,ReencCount,ReencTimeMs,ReencDeltaBytes,ReencAfterBytes," +
                                "RatioDenomSource,TokenK,TokenKBase,qIndexZeroBased,cand_metrics_mode," +
                                "CF_req,CF_ret,SF,SF_Touched,SF_Reenc\n"
                );

                for (String[] row : snapshot) {
                    bw.write(String.join(",", row));
                    bw.write("\n");
                }
            }
            logger.info("Top-K evaluation written to {}", outPath);
        } catch (IOException ex) {
            logger.error("Failed to write top-K evaluation CSV to {}", outPath, ex);
            throw new RuntimeException("Failed to write CSV: " + outPath, ex);
        }
    }

    // -------------------------------------------------------------------------
    // Internals
    // -------------------------------------------------------------------------

    private void recordInternal(String queryId,
                                List<QueryEvaluationResult> results,
                                Integer candTotalOv,
                                Integer candKeptOv,
                                Integer candDecOv,
                                Integer candRetOv) {
        Objects.requireNonNull(queryId, "Query ID cannot be null");
        Objects.requireNonNull(results, "Results cannot be null");

        final String qid = csvEscape(queryId);

        synchronized (rows) {
            for (QueryEvaluationResult r : results) {

                // Candidate pipeline (prefer in-object values, else override)
                final int topK          = safePos(r.getTopKRequested());
                final int returned      = nonNeg(coalesceNonZero(r.getRetrieved(), candRetOv));
                final int candTotal     = nonNeg(coalesceNonZero(r.getCandTotal(), candTotalOv));
                final int candKept      = nonNeg(coalesceNonZero(r.getCandKept(), candKeptOv));
                final int candDecrypted = nonNeg(coalesceNonZero(r.getCandDecrypted(), candDecOv));
                final int candReturned  = nonNeg(coalesceNonZero(r.getCandReturned(), candRetOv));

                // Times
                final long serverMs  = Math.max(0L, r.getTimeMs());
                final long clientMs  = (r.getClientTimeMs() < 0 ? -1L : r.getClientTimeMs());
                final long runMs     = Math.max(0L, r.getRunTimeMs());
                final long decryptMs = Math.max(0L, r.getDecryptTimeMs());
                final long insertMs  = Math.max(0L, r.getInsertTimeMs());

                // Precision + Ratio
                final String ratioStr     = Double.isNaN(r.getRatio())     ? "NaN" : fmt4(r.getRatio());
                final String precisionStr = Double.isNaN(r.getPrecision()) ? "NaN" : fmt4(r.getPrecision());

                // Derived factors
                final String cfReq = (topK > 0 && candTotal >= 0)   ? fmt6(div(candTotal, topK))   : "NaN";
                final String cfRet = (returned > 0 && candTotal >= 0)? fmt6(div(candTotal, returned)): "NaN";
                final String sfEval    = (datasetSize > 0 && candTotal >= 0) ? fmt6(div(candTotal, datasetSize)) : "NaN";
                final String sfTouched = (datasetSize > 0 && r.getTouchedCount() >= 0) ? fmt6(div(r.getTouchedCount(), datasetSize)) : "NaN";
                final String sfReenc   = (datasetSize > 0 && r.getReencryptedCount() >= 0) ? fmt6(div(r.getReencryptedCount(), datasetSize)) : "NaN";

                rows.add(new String[]{
                        qid, String.valueOf(topK), String.valueOf(returned),
                        ratioStr, precisionStr,
                        String.valueOf(serverMs), String.valueOf(clientMs),
                        String.valueOf(runMs), String.valueOf(decryptMs), String.valueOf(insertMs),
                        String.valueOf(candTotal), String.valueOf(candKept),
                        String.valueOf(candDecrypted), String.valueOf(candReturned),
                        String.valueOf(r.getTokenSizeBytes()), String.valueOf(r.getVectorDim()),
                        String.valueOf(r.getTouchedCount()), String.valueOf(r.getReencryptedCount()),
                        String.valueOf(r.getReencTimeMs()), String.valueOf(r.getReencBytesDelta()), String.valueOf(r.getReencBytesAfter()),
                        nz(r.getRatioDenomSource()), String.valueOf(r.getTokenK()), String.valueOf(r.getTokenKBase()),
                        String.valueOf(r.getQIndexZeroBased()), nz(r.getCandMetricsMode()),
                        cfReq, cfRet, sfEval, sfTouched, sfReenc
                });
            }
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static int safePos(int v) { return v > 0 ? v : 0; }

    private static String csvEscape(String s) {
        if (s == null) return "";
        boolean needsQuotes = s.indexOf(',') >= 0 || s.indexOf('"') >= 0 || s.indexOf('\n') >= 0 || s.indexOf('\r') >= 0;
        if (!needsQuotes) return s;
        String escaped = s.replace("\"", "\"\"");
        return "\"" + escaped + "\"";
    }

    private static String nz(String s) { return (s == null || s.isBlank()) ? "none" : s; }

    private static int nonNeg(int v) { return Math.max(0, v); }

    private static int coalesceNonZero(int primary, Integer overrideIfZero) {
        if (primary > 0) return primary;
        if (overrideIfZero != null && overrideIfZero > 0) return overrideIfZero;
        return primary; // zero or negative stays as-is
    }

    private static double div(long a, long b) { return (b <= 0) ? Double.NaN : ((double) a / (double) b); }

    private static String fmt4(double v) { return String.format(Locale.ROOT, "%.4f", v); }
    private static String fmt6(double v) { return String.format(Locale.ROOT, "%.6f", v); }

    /** Clear accumulated rows (e.g., between runs). */
    public void reset() { synchronized (rows) { rows.clear(); } }

    /** Current number of recorded rows. */
    public int size() { synchronized (rows) { return rows.size(); } }
}
