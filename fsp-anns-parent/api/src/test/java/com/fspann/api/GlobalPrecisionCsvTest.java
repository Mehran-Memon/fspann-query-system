package com.fspann.api;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class GlobalPrecisionCsvTest {

    @TempDir
    Path tmp;

    @Test
    void writesSinglePrecisionColumn_withExpectedHeaderAndValues() throws Exception {
        // Arrange: small synthetic stats
        int dim = 128;
        int[] kVariants = {10};
        Map<Integer, Long> globalMatches = Map.of(10, 7L);   // 7 correct
        Map<Integer, Long> globalRetrieved = Map.of(10, 10L); // 10 retrieved
        int queriesCount = 5; // so return_rate = 10 / (5 * 10) = 0.2

        // Invoke private static method via reflection
        Method m = ForwardSecureANNSystem.class.getDeclaredMethod(
                "writeGlobalPrecisionCsv",
                Path.class,
                int.class,
                int[].class,
                Map.class,
                Map.class,
                int.class
        );
        m.setAccessible(true);
        m.invoke(null, tmp, dim, kVariants, globalMatches, globalRetrieved, queriesCount);

        // Read CSV
        Path csv = tmp.resolve("global_precision.csv");
        assertTrue(Files.exists(csv), "global_precision.csv should exist");

        List<String> lines = Files.readAllLines(csv);
        assertEquals(2, lines.size(), "Expected header + one data row");

        String header = lines.get(0);
        assertEquals(
                "dimension,topK,precision,return_rate,matches_sum,returned_sum,queries",
                header
        );

        String[] cols = lines.get(1).split(",");
        assertEquals("128", cols[0]);
        assertEquals("10", cols[1]);
        assertEquals("0.700000", cols[2], "precision should be 7/10 = 0.7");
        assertEquals("0.200000", cols[3], "return_rate should be 10 / (5 * 10) = 0.2");
        assertEquals("7", cols[4]);
        assertEquals("10", cols[5]);
        assertEquals("5", cols[6]);
    }
}
