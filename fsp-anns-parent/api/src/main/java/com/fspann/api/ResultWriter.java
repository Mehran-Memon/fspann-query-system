package com.fspann.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.*;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class ResultWriter {
    private static final Logger logger = LoggerFactory.getLogger(ResultWriter.class);
    private final Path outputPath;

    public ResultWriter(Path outputPath) {
        Objects.requireNonNull(outputPath, "Output path cannot be null");
        Path normalizedPath = outputPath.normalize();
        Path basePath = Paths.get("results").normalize();
        if (!normalizedPath.startsWith(basePath)) {
            logger.error("Path traversal detected: {}", normalizedPath);
            throw new IllegalArgumentException("Invalid output path: " + normalizedPath);
        }
        this.outputPath = normalizedPath;
    }

    public void writeTable(String title, String[] columns, List<String[]> rows) throws IOException {
        Objects.requireNonNull(title, "Title cannot be null");
        Objects.requireNonNull(columns, "Columns cannot be null");
        Objects.requireNonNull(rows, "Rows cannot be null");

        Files.createDirectories(outputPath.getParent());
        if (!Files.isWritable(outputPath.getParent())) {
            logger.error("Output directory is not writable: {}", outputPath.getParent());
            throw new IOException("Output directory is not writable: " + outputPath.getParent());
        }

        try (BufferedWriter writer = Files.newBufferedWriter(outputPath, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            writer.write(title + "\n");
            writer.write(String.join("\t", columns) + "\n");

            for (String[] row : rows) {
                writer.write(String.join("\t", row));
                writer.write("\n");
            }

            writer.write("\n\n");
            logger.info("Results written to {}", outputPath);
        } catch (IOException e) {
            logger.error("Failed to write results to {}", outputPath, e);
            throw new IOException("Failed to write results to " + outputPath, e);
        }
    }
}