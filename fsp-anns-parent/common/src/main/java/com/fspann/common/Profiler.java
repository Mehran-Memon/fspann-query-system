package com.fspann.common;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Simple profiler for measuring durations and exporting results.
 */
public class Profiler {
    private final Map<String, Long> startTimes = new HashMap<>();
    private final Map<String, List<Long>> timings = new HashMap<>();
    private final List<QueryMetric> queryMetrics = new ArrayList<>();
    private final List<Double> clientQueryTimes = new ArrayList<>();
    private final List<Double> serverQueryTimes = new ArrayList<>();
    private final List<Double> queryRatios = new ArrayList<>();
    private final List<String[]> topKRecords = new ArrayList<>();

    public void start(String label) {
        startTimes.put(label, System.nanoTime());
    }

    public void stop(String label) {
        long end = System.nanoTime();
        long start = startTimes.getOrDefault(label, end);
        long duration = end - start;
        timings.computeIfAbsent(label, k -> new ArrayList<>()).add(duration);
    }

    public void log(String label) {
        List<Long> all = timings.get(label);
        if (all == null || all.isEmpty()) return;
        double avg = all.stream().mapToLong(Long::longValue).average().orElse(0) / 1_000_000.0;
        System.out.printf("⏱️ %s: avg %.2f ms (over %d runs)%n", label, avg, all.size());
    }

    public void logMemory(String label) {
        Runtime runtime = Runtime.getRuntime();
        long used = runtime.totalMemory() - runtime.freeMemory();
        System.out.printf("📦 %s: %.2f MB RAM%n", label, used / 1024.0 / 1024.0);
    }

    public void exportToCSV(String filePath) {
        try (FileWriter fw = new FileWriter(filePath)) {
            fw.write("Label,AvgTime(ms),Runs\n");
            for (Map.Entry<String, List<Long>> e : timings.entrySet()) {
                double avg = e.getValue().stream().mapToLong(Long::longValue).average().orElse(0) / 1_000_000.0;
                fw.write(String.format("%s,%.4f,%d\n", e.getKey(), avg, e.getValue().size()));
            }
            System.out.println("📤 Profiler data written to " + filePath);
        } catch (IOException ex) {
            System.err.println("Failed to write profiler CSV: " + ex.getMessage());
        }
    }


    public void recordTopKVariants(String queryId, List<QueryEvaluationResult> results) {
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

    public void exportTopKVariants(String filePath) {
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


    public List<Long> getTimings(String label) {
        return timings.getOrDefault(label, Collections.emptyList());
    }

    public void recordQueryMetric(String label, double serverMs, double clientMs, double ratio) {
        serverQueryTimes.add(serverMs);
        clientQueryTimes.add(clientMs);
        queryRatios.add(ratio);
    }

    public void exportQueryMetrics(String filePath) {
        try (FileWriter fw = new FileWriter(filePath)) {
            fw.write("Query,ServerART(ms),ClientART(ms),Ratio\n");
            for (int i = 0; i < clientQueryTimes.size(); i++) {
                fw.write(String.format("Q%d,%.4f,%.4f,%.4f\n",
                        i + 1,
                        serverQueryTimes.get(i),
                        clientQueryTimes.get(i),
                        queryRatios.get(i)));
            }
            System.out.println("📤 Query metrics written to " + filePath);
        } catch (IOException ex) {
            System.err.println("Failed to write query metrics CSV: " + ex.getMessage());
        }
    }


    public List<Double> getAllClientQueryTimes() {
        return clientQueryTimes;
    }

    public List<Double> getAllServerQueryTimes() {
        return serverQueryTimes;
    }

    public List<Double> getAllQueryRatios() {
        return queryRatios;
    }

    private static class QueryMetric {
        String id;
        double serverMs;
        double clientMs;
        double ratio;

        public QueryMetric(String id, double serverMs, double clientMs, double ratio) {
            this.id = id;
            this.serverMs = serverMs;
            this.clientMs = clientMs;
            this.ratio = ratio;
        }
    }


}