package com.fspann.api;

import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

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
}