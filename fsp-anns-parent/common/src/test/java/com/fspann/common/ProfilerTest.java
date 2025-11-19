package com.fspann.common;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ProfilerTest {
    @TempDir Path tempDir;

    @Test
    void timingAndExportCsv() throws IOException {
        Profiler profiler = new Profiler();
        profiler.start("op1");
        profiler.stop("op1");

        Path csv = tempDir.resolve("prof.csv");
        profiler.exportToCSV(csv.toString());
        assertTrue(Files.exists(csv));

        List<String> lines = Files.readAllLines(csv);
        assertEquals("Label,AvgTime(ms),Runs", lines.get(0));
        assertTrue(lines.get(1).startsWith("op1,"));
    }

}

