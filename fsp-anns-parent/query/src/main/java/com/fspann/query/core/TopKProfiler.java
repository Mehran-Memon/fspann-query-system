package com.fspann.query.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class TopKProfiler {
    private static final Logger logger = LoggerFactory.getLogger(TopKProfiler.class);
    private final List<String[]> topKRecords = new ArrayList<>();
    private final String baseDir;

    public TopKProfiler(String baseDir) {
        this.baseDir = Objects.requireNonNull(baseDir, "Base directory cannot be null");
    }

    public void record(String queryId, List<QueryEvaluationResult> results) {
        Objects.requireNonNull(queryId, "Query ID cannot be null");
        Objects.requireNonNull(results, "Results cannot be null");

        for (QueryEvaluationResult r : results) {
            topKRecords.add(new String[]{
                    queryId,
                    String.valueOf(r.getTopKRequested()),
                    String.valueOf(r.getRetrieved()),
                    String.format("%.4f", r.getRatio()),
                    String.format("%.4f", r.getRecall()),
                    String.valueOf(r.getTimeMs()),
                    String.valueOf(r.getInsertTimeMs()),
                    String.valueOf(r.getCandidateCount()),
                    String.valueOf(r.getTokenSizeBytes()),
                    String.valueOf(r.getVectorDim())
            });
        }
    }

    public void export(String filePath) {
        Objects.requireNonNull(filePath, "File path cannot be null");
        Path basePath = Paths.get(baseDir).normalize().toAbsolutePath();
        Path outPath = Paths.get(filePath);

        if (!outPath.isAbsolute()) {
            outPath = basePath.resolve(outPath);
        }
        outPath = outPath.normalize().toAbsolutePath();

        if (!outPath.startsWith(basePath)) {
            logger.error("Export path {} is outside profiler baseDir {}", outPath, basePath);
            throw new IllegalArgumentException("Invalid export path: " + filePath);
        }

        try {
            Files.createDirectories(outPath.getParent());
            try (BufferedWriter bw = Files.newBufferedWriter(outPath)) {
                // CSV header including new metrics
                bw.write("QueryID,TopK,Retrieved,Ratio,Recall,TimeMs,InsertTimeMs,CandidateCount,TokenSizeBytes,VectorDim\n");
                for (String[] row : topKRecords) {
                    bw.write(String.join(",", row) + "\n");
                }
            }
            logger.info("Top-K evaluation written to {}", outPath);
        } catch (IOException ex) {
            logger.error("Failed to write top-K evaluation CSV to {}", outPath, ex);
            throw new RuntimeException("Failed to write CSV: " + outPath, ex);
        }
    }
}
