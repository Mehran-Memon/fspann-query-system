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

        // Insert ONE fake query row (minimal valid row)
        profiler.recordQueryRow(
                "op1",      // label
                10.0,       // serverMs
                20.0,       // clientMs
                30.0,       // runMs
                5.0,        // decryptMs
                2.0,        // insertMs
                1.5,        // ratio
                0.8,        // precision
                100,        // candTotal
                80,         // candKept
                40,         // candDecrypted
                20,         // candReturned
                0,          // tokenBytes
                128,        // vectorDim
                100,        // tokenK
                100,        // tokenKBase
                0,          // qIndex
                0,          // totalFlushed
                0,          // flushThreshold
                0,          // touchedCount
                0,          // reencCount
                0L,         // reencTimeMs
                0L,         // reencBytesDelta
                0L,         // reencBytesAfter
                "auto",     // ratioDenomSource
                "full",      // mode
                0,
                0
        );

        Path csv = tempDir.resolve("prof.csv");
        profiler.exportToCSV(csv.toString());
        assertTrue(Files.exists(csv), "CSV file should have been created");

        List<String> lines = Files.readAllLines(csv);
        assertFalse(lines.isEmpty(), "CSV should not be empty");

        // Header must match new Profiler.exportQueryMetricsCsv()
        assertEquals("label,serverMs,clientMs,ratio", lines.get(0));

        assertTrue(lines.size() >= 2, "CSV should contain at least one data row");

        String data = lines.get(1);
        assertTrue(data.startsWith("op1,"), "First data row should start with 'op1,'");

        String[] parts = data.split(",", -1);
        assertEquals(4, parts.length, "Expected 4 CSV columns: label,serverMs,clientMs,ratio");
    }
}
