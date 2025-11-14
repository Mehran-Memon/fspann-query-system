package com.fspann.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.*;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static java.nio.file.StandardOpenOption.*;

/**
 * Utility for writing evaluation tables (metrics, summaries, etc.) to a file.
 *
 * Features:
 *  - Supports both CSV and simple tab-separated text.
 *  - For CSV mode, prepends a "Section" column so multiple logical tables
 *    can coexist in a single file (e.g., "TopKMetrics", "Summary", "ReencStats").
 *  - Avoids rewriting headers when appending to an existing CSV.
 *  - Logs a warning if subsequent writes attempt to change the CSV header schema.
 */
public class ResultWriter {
    private static final Logger logger = LoggerFactory.getLogger(ResultWriter.class);

    private final Path outputPath;
    private final boolean csvMode;

    /** True once the header has been written (or detected as existing). */
    private boolean wroteHeader = false;

    /** Last header used in CSV mode, to detect schema drift. */
    private String[] lastHeader = null;

    public ResultWriter(Path outputPath) {
        Objects.requireNonNull(outputPath, "Output path cannot be null");
        this.outputPath = outputPath.toAbsolutePath().normalize();
        this.csvMode = this.outputPath.toString()
                .toLowerCase(Locale.ROOT)
                .endsWith(".csv");

        // If appending to an existing CSV file, don’t write the header again.
        if (csvMode) {
            try {
                this.wroteHeader = Files.exists(this.outputPath) && Files.size(this.outputPath) > 0;
            } catch (IOException ignore) {
                this.wroteHeader = false;
            }
        }
    }

    /**
     * Write a logical table into the output file.
     *
     * @param title   Logical section name, used as the "Section" column in CSV mode.
     * @param columns Column headers (without "Section"; that is added automatically in CSV).
     * @param rows    Data rows (must align with the number of columns).
     */
    public void writeTable(String title, String[] columns, List<String[]> rows) throws IOException {
        Objects.requireNonNull(title, "Title cannot be null");
        Objects.requireNonNull(columns, "Columns cannot be null");
        Objects.requireNonNull(rows, "Rows cannot be null");

        Path parent = outputPath.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        try (BufferedWriter writer = Files.newBufferedWriter(outputPath, CREATE, APPEND)) {
            if (csvMode) {
                // In CSV mode, we have a single global header: "Section" + user columns.
                if (!wroteHeader) {
                    // Store header schema for consistency checks.
                    lastHeader = columns.clone();
                    writer.write("Section," + String.join(",", escape(columns)) + "\n");
                    wroteHeader = true;
                } else {
                    // Header already exists; check for schema drift.
                    if (lastHeader != null && !columnsCompatible(lastHeader, columns)) {
                        logger.warn("ResultWriter: header schema changed between writes. "
                                        + "Existing header: {}, new header: {}",
                                String.join("|", lastHeader),
                                String.join("|", columns));
                    }
                }

                for (String[] row : rows) {
                    // Best-effort safety: align row length with header length.
                    String[] normalized = normalizeRow(row, columns.length);
                    writer.write(escape(title));
                    writer.write(",");
                    writer.write(String.join(",", escape(normalized)));
                    writer.write("\n");
                }
            } else {
                // Legacy pretty text (tab-separated) for human inspection.
                writer.write(title);
                writer.write("\n");
                writer.write(String.join("\t", columns));
                writer.write("\n");
                for (String[] row : rows) {
                    writer.write(String.join("\t", row));
                    writer.write("\n");
                }
                writer.write("\n\n");
            }

            logger.info("Results written to {}", outputPath);
        } catch (IOException e) {
            logger.error("Failed to write results to {}", outputPath, e);
            throw new IOException("Failed to write results to " + outputPath, e);
        }
    }

    /* -------------------- helpers -------------------- */

    private static String[] escape(String[] cells) {
        String[] out = new String[cells.length];
        for (int i = 0; i < cells.length; i++) {
            out[i] = escape(cells[i]);
        }
        return out;
    }

    private static String escape(String s) {
        if (s == null) return "";
        boolean needs = s.indexOf(',') >= 0
                || s.indexOf('"') >= 0
                || s.indexOf('\n') >= 0
                || s.indexOf('\r') >= 0;
        return needs ? "\"" + s.replace("\"", "\"\"") + "\"" : s;
    }

    /**
     * Normalize a row to the given length:
     *  - If too short, pad with empty strings.
     *  - If too long, truncate.
     */
    private static String[] normalizeRow(String[] row, int expectedLen) {
        if (row == null) {
            String[] empty = new String[expectedLen];
            for (int i = 0; i < expectedLen; i++) empty[i] = "";
            return empty;
        }
        if (row.length == expectedLen) return row;

        String[] out = new String[expectedLen];
        int copyLen = Math.min(row.length, expectedLen);
        System.arraycopy(row, 0, out, 0, copyLen);
        for (int i = copyLen; i < expectedLen; i++) {
            out[i] = "";
        }
        return out;
    }

    /**
     * Very light header-compatibility check: same length and same names.
     * We only log a warning if they differ; no exception thrown.
     */
    private static boolean columnsCompatible(String[] previous, String[] current) {
        if (previous.length != current.length) return false;
        for (int i = 0; i < previous.length; i++) {
            if (!Objects.equals(previous[i], current[i])) return false;
        }
        return true;
    }
}
