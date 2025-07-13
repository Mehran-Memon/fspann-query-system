package com.fspann.utils;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class Profiler {
    private final Map<String, Long> startTimes = new HashMap<>();
    private final Map<String, List<Long>> timings = new HashMap<>();

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
        double avg = all.stream().mapToLong(Long::longValue).average().orElse(0);
        System.out.printf("⏱️ %s: avg %.2f ms (over %d runs)%n", label, avg / 1_000_000.0, all.size());
    }

    public void logMemory(String label) {
        Runtime runtime = Runtime.getRuntime();
        long used = runtime.totalMemory() - runtime.freeMemory();
        System.out.printf("📦 %s: %.2f MB RAM%n", label, used / 1024.0 / 1024.0);
    }

    public void exportToCSV(String filePath) {
        try (FileWriter fw = new FileWriter(filePath)) {
            fw.write("Label,AvgTime(ms),Runs\n");
            for (String label : timings.keySet()) {
                List<Long> data = timings.get(label);
                double avg = data.stream().mapToLong(Long::longValue).average().orElse(0) / 1_000_000.0;
                fw.write(String.format("%s,%.4f,%d\n", label, avg, data.size()));
            }
            System.out.println("📤 Profiler data written to " + filePath);
        } catch (IOException e) {
            System.err.println("Failed to write profiler CSV: " + e.getMessage());
        }
    }
}
