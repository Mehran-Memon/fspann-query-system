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

    public TopKProfiler(String baseDir) {
        this.baseDir = Objects.requireNonNull(baseDir, "Base directory cannot be null");
    }

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
                    // Literature ratio (paper): stored in r.getRatio()
                    (Double.isNaN(r.getRatio()) ? "NaN" : String.format(Locale.ROOT, "%.4f", r.getRatio())),
                    // Precision@K: stored in r.getRecall() field by design
                    (Double.isNaN(r.getRecall()) ? "NaN" : String.format(Locale.ROOT, "%.4f", r.getRecall())),
                    String.valueOf(r.getTimeMs()),
                    String.valueOf(r.getInsertTimeMs()),
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
                bw.write("QueryID,TopK,Retrieved,LiteratureRatio,Precision,TimeMs,InsertTimeMs,CandidateCount,TokenSizeBytes,VectorDim,"
                        + "CandTotal,CandKeptVersion,CandDecrypted,Returned\n");
                for (String[] row : rows) {
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
}
