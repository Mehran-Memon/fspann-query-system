package com.fspann.query.core;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TopKProfiler {
    private final List<String[]> topKRecords = new ArrayList<>();

    public void record(String queryId, List<QueryEvaluationResult> results) {
        for (QueryEvaluationResult r : results) {
            topKRecords.add(new String[]{
                    queryId,
                    String.valueOf(r.getTopKRequested()),
                    String.valueOf(r.getRetrieved()),
                    String.format("%.4f", r.getRatio()),
                    String.format("%.4f", r.getRecall()),
                    String.valueOf(r.getTimeMs())
            });
        }
    }

    public void export(String filePath) {
        try (FileWriter fw = new FileWriter(filePath)) {
            fw.write("QueryID,TopK,Retrieved,Ratio,Recall,TimeMs\n");
            for (String[] row : topKRecords) {
                fw.write(String.join(",", row) + "\n");
            }
            System.out.println("📤 Top-K evaluation written to " + filePath);
        } catch (IOException ex) {
            System.err.println("Failed to write top-K evaluation CSV: " + ex.getMessage());
        }
    }
}
