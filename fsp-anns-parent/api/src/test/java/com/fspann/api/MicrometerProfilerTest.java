package com.fspann.api;

import com.fspann.common.Profiler;
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

        // start and stop a label
        prof.start("test-op");
        prof.stop("test-op");

        // export to CSV
        Path csv = tempDir.resolve("profiler.csv");
        prof.exportToCSV(csv.toString());
        assertTrue(Files.exists(csv), "CSV file should exist");

        // basic CSV content check
        try (BufferedReader r = Files.newBufferedReader(csv)) {
            String header = r.readLine();
            assertEquals("Label,AvgTime(ms),Runs", header);
            String data = r.readLine();
            assertNotNull(data, "There should be at least one data line");
            assertTrue(data.startsWith("test-op,"), "Data line should start with label");
        } catch (IOException e) {
            fail("Failed to read CSV: " + e.getMessage());
        }
    }
}
