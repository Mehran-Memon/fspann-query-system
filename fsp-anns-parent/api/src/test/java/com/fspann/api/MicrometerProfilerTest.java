package com.fspann.api;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MicrometerProfilerTest {

    @TempDir Path tempDir;

    @Test
    void timingEntriesGetRecorded() {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        MicrometerProfiler prof = new MicrometerProfiler(registry);

        // Start and stop a label
        prof.start("test-op");
        try {
            Thread.sleep(50); // Simulate some work
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        prof.stop("test-op");

        // Export to CSV
        Path csv = tempDir.resolve("profiler.csv");
        prof.exportToCSV(csv.toString());
        assertTrue(Files.exists(csv), "CSV file should exist");

        // Verify CSV content
        try (BufferedReader r = Files.newBufferedReader(csv)) {
            String header = r.readLine();
            assertEquals("Label,AvgTime(ms),Runs", header, "CSV header should match expected format");
            String data = r.readLine();
            assertNotNull(data, "There should be at least one data line");
            assertTrue(data.startsWith("test-op,"), "Data line should start with label");
            String[] parts = data.split(",");
            double avgTime = Double.parseDouble(parts[1]);
            assertTrue(avgTime >= 50, "Average time should be at least 50ms");
            assertEquals("1", parts[2], "Should have exactly 1 run");
        } catch (IOException e) {
            fail("Failed to read CSV: " + e.getMessage());
        }
    }

    @Test
    void averageRatio_usesOnlyTrueQueryLabels() {
        MicrometerProfiler p = new MicrometerProfiler(new SimpleMeterRegistry());

        // Non-query label → ignored by TRUE_QUERY ("^Q\\d+$")
        p.recordQueryMetric("warmup", 0, 0, 9.9);

        // True per-query rows
        p.recordQueryMetric("Q0", 12, 15, 1.10);
        p.recordQueryMetric("Q1", 10, 12, 1.30);

        // Cache/cloak style labels should be ignored by the tightened filter
        p.recordQueryMetric("Q_cache_20", 0, 1, 0.00);
        p.recordQueryMetric("Q_cloak_10", 0, 1, 0.00);

        List<Double> ratios = p.getAllQueryRatios();
        assertEquals(2, ratios.size(), "Only Q<digits> should be tracked");
        assertEquals(1.10, ratios.get(0), 1e-9);
        assertEquals(1.30, ratios.get(1), 1e-9);

        double avg = ratios.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        assertEquals(1.20, avg, 1e-9);
    }

    @Test
    void exportQueryMetricsCSV_includesAllRows_forDiagnostics() throws Exception {
        var p = new MicrometerProfiler(new SimpleMeterRegistry());
        p.recordQueryMetric("Q0", 10, 12, 1.1);
        p.recordQueryMetric("warmup", 1, 1, 9.9);       // will be present in CSV
        p.recordQueryMetric("Q_cache_20", 0, 1, 0.0);   // present in CSV

        java.nio.file.Path tmp = java.nio.file.Files.createTempFile("qm", ".csv");
        p.exportQueryMetricsCSV(tmp.toString());

        String csv = java.nio.file.Files.readString(tmp);
        assertTrue(csv.contains("Q0,10.000,12.000,1.100000"));
        assertTrue(csv.contains("warmup,1.000,1.000,9.900000"));
        assertTrue(csv.contains("Q_cache_20,0.000,1.000,0.000000"));
    }
}