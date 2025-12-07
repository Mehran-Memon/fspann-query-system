package com.fspann.query.core;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import static org.junit.jupiter.api.Assertions.*;

class EvaluationSummaryPrinterTest {

    @Test
    void writesAllCsvs(@TempDir Path dir) throws Exception {
        Aggregates ag = new Aggregates();
        ag.avgRatio = 0.5;
        ag.avgServerMs = 10;
        ag.avgClientMs = 20;

        Path summary = dir.resolve("summary.csv");

        EvaluationSummaryPrinter.printAndWriteCsv(
                "datasetX",
                "ideal-system",
                8,4,3,
                123,
                ag,
                summary
        );

        assertTrue(Files.exists(summary));
        assertTrue(Files.exists(summary.resolveSibling("accuracy.csv")));
        assertTrue(Files.exists(summary.resolveSibling("cost.csv")));
    }
}
