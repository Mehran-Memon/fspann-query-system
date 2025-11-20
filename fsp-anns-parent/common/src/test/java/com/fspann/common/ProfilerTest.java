// File: src/test/java/com/fspann/common/ProfilerTest.java
package com.fspann.common;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ProfilerTest {

    @TempDir
    Path tempDir;

    @Test
    void timingAndExportCsv() throws IOException {
        Profiler profiler = new Profiler();

        // Ensure we have at least one query-metric row in the CSV
        profiler.recordQueryMetric("op1", 10.0, 20.0, 1.5);

        Path csv = tempDir.resolve("prof.csv");
        profiler.exportToCSV(csv.toString());
        assertTrue(Files.exists(csv), "CSV file should have been created");

        List<String> lines = Files.readAllLines(csv);
        assertFalse(lines.isEmpty(), "CSV should not be empty");

        // New header produced by Profiler.exportToCSV()
        assertEquals("label,serverMs,clientMs,ratio", lines.get(0));

        assertTrue(lines.size() >= 2, "CSV should contain at least one data row");
        String data = lines.get(1);

        // First column: label
        assertTrue(data.startsWith("op1,"), "First data row should start with 'op1,'");

        // Rough shape check: 4 columns separated by commas
        String[] parts = data.split(",", -1);
        assertEquals(4, parts.length, "Expected 4 CSV columns: label,serverMs,clientMs,ratio");
    }
}
