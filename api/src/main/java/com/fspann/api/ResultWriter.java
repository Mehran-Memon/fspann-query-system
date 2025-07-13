package com.fspann.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.*;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.List;

/**
 * Helper to write result tables for experiments.
 */
public class ResultWriter {
    private static final Logger logger = LoggerFactory.getLogger(ResultWriter.class);
    private final Path outputPath;

    public ResultWriter(Path outputPath) {
        this.outputPath = outputPath;
    }

    public void writeTable(String title, String[] columns, List<String[]> rows) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(outputPath, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            writer.write(title + "\n");
            writer.write(String.join("\t", columns) + "\n");

            for (String[] row : rows) {
                writer.write(String.join("\t", row));
                writer.write("\n");
            }

            writer.write("\n\n");
        }
        logger.info("Results written to {}", outputPath);
    }
}

