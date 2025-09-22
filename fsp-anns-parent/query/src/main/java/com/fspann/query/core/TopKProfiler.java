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
    // New: used to compute SF (scanned fraction)
    private volatile long datasetSize = -1L;

    public TopKProfiler(String baseDir) {
        this.baseDir = Objects.requireNonNull(baseDir, "Base directory cannot be null");
    }

    /** New: let the exporter compute SF = candidates / N. Safe to skip; SF becomes NaN. */
    public void setDatasetSize(long n) { this.datasetSize = n; }

    public void record(String queryId,
                       List<QueryEvaluationResult> results,
                       int candTotal, int candKeptVersion, int candDecrypted, int returned) {
        Objects.requireNonNull(queryId, "Query ID cannot be null");
        Objects.requireNonNull(results, "Results cannot be null");

        for (QueryEvaluationResult r : results) {
            rows.add(new String[]{
                    queryId,
                    String.valueOf(r.getTopKRequested()),
                    String.valueOf(r.getRetrieved()),
                    (Double.isNaN(r.getRatio()) ? "NaN" : String.format(Locale.ROOT, "%.4f", r.getRatio())),
                    (Double.isNaN(r.getPrecision()) ? "NaN" : String.format(Locale.ROOT, "%.4f", r.getPrecision())),
                    String.valueOf(r.getTimeMs()),
                    String.valueOf(r.getInsertTimeMs()),
                    // CandidateCount must be the per-K unique fully-evaluated count (caller sets it)
                    String.valueOf(r.getCandidateCount()),
                    String.valueOf(r.getTokenSizeBytes()),
                    String.valueOf(r.getVectorDim()),
                    String.valueOf(candTotal),
                    String.valueOf(candKeptVersion),
                    String.valueOf(candDecrypted),
                    String.valueOf(returned)
            });
        }
    }

    public void export(String filePath) {
        Objects.requireNonNull(filePath, "File path cannot be null");

        Path basePath = Paths.get(baseDir).normalize().toAbsolutePath();
        Path outPath = Paths.get(filePath);
        if (!outPath.isAbsolute()) outPath = basePath.resolve(outPath);
        outPath = outPath.normalize().toAbsolutePath();

        if (!outPath.startsWith(basePath)) {
            logger.error("Export path {} is outside profiler baseDir {}", outPath, basePath);
            throw new IllegalArgumentException("Invalid export path: " + filePath);
        }

        try {
            Files.createDirectories(outPath.getParent());
            try (BufferedWriter bw = Files.newBufferedWriter(outPath)) {
                // Added fanout columns: CF_req, CF_ret, SF
                bw.write(
                        "QueryID,TopK,Retrieved,Ratio,Precision,TimeMs,InsertTimeMs,CandidateCount,TokenSizeBytes,VectorDim," +
                                "CandTotal,CandKeptVersion,CandDecrypted,Returned,CF_req,CF_ret,SF\n"
                );

                for (String[] row : rows) {
                    int topK      = parseIntSafe(row[1], 0);
                    int retrieved = parseIntSafe(row[2], 0);
                    int candCount = parseIntSafe(row[7], 0);

                    String cfReq = (topK > 0 && candCount >= 0)
                            ? fmtDouble((double)candCount / (double)topK) : "NaN";
                    String cfRet = (retrieved > 0 && candCount >= 0)
                            ? fmtDouble((double)candCount / (double)retrieved) : "NaN";
                    String sf    = (datasetSize > 0 && candCount >= 0)
                            ? fmtDouble((double)candCount / (double)datasetSize) : "NaN";

                    bw.write(String.join(",", row));
                    bw.write(",");
                    bw.write(cfReq); bw.write(",");
                    bw.write(cfRet); bw.write(",");
                    bw.write(sf);
                    bw.write("\n");
                }
            }
            logger.info("Top-K evaluation written to {}", outPath);
        } catch (IOException ex) {
            logger.error("Failed to write top-K evaluation CSV to {}", outPath, ex);
            throw new RuntimeException("Failed to write CSV: " + outPath, ex);
        }
    }

    private static String fmtDouble(double v) { return String.format(Locale.ROOT, "%.6f", v); }
    private static int parseIntSafe(String s, int def) {
        try { return Integer.parseInt(s); } catch (Exception ignore) { return def; }
    }
}
