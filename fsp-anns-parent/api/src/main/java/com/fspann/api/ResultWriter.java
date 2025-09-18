package com.fspann.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.*;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import static java.nio.file.StandardOpenOption.*;

public class ResultWriter {
    private static final Logger logger = LoggerFactory.getLogger(ResultWriter.class);
    private static final Pattern QIDX =
            Pattern.compile("\\b(?:Q(?:uery)?\\s*[=:]?\\s*)(\\d+)\\b", Pattern.CASE_INSENSITIVE);

    private final Path outputPath;
    private final boolean csvMode;
    private boolean wroteHeader = false;

    public ResultWriter(Path outputPath) {
        Objects.requireNonNull(outputPath, "Output path cannot be null");
        this.outputPath = outputPath.toAbsolutePath().normalize();
        this.csvMode = this.outputPath.toString().toLowerCase(Locale.ROOT).endsWith(".csv");

        // If appending to an existing CSV file, don’t write the header again.
        if (csvMode) {
            try {
                this.wroteHeader = Files.exists(this.outputPath) && Files.size(this.outputPath) > 0;
            } catch (IOException ignore) {
                this.wroteHeader = false;
            }
        }
    }

    public void writeTable(String title, String[] columns, List<String[]> rows) throws IOException {
        Objects.requireNonNull(title, "Title cannot be null");
        Objects.requireNonNull(columns, "Columns cannot be null");
        Objects.requireNonNull(rows, "Rows cannot be null");

        Files.createDirectories(outputPath.getParent());

        try (BufferedWriter writer = Files.newBufferedWriter(outputPath, CREATE, APPEND)) {
            if (csvMode) {
                if (!wroteHeader) {
                    // Prepend a qIndex + Section column so multiple blocks can co-exist in one CSV.
                    writer.write("qIndex,Section," + String.join(",", escape(columns)) + "\n");
                    wroteHeader = true;
                }
                final int qIndex = parseQIndexFromTitle(title); // e.g., "Query 7 Results ..." → 7 (or -1 if none)
                for (String[] row : rows) {
                    writer.write(Integer.toString(qIndex));
                    writer.write(",");
                    writer.write(escape(title));
                    writer.write(",");
                    writer.write(String.join(",", escape(row)));
                    writer.write("\n");
                }
            } else {
                // legacy pretty text (tab-separated)
                writer.write(title + "\n");
                writer.write(String.join("\t", columns) + "\n");
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

    private static int parseQIndexFromTitle(String title) {
        if (title == null) return -1;
        Matcher m = QIDX.matcher(title);
        if (m.find()) {
            try { return Integer.parseInt(m.group(1)); }
            catch (NumberFormatException ignore) { /* fall through */ }
        }
        return -1; // unknown / not present
    }

    private static String[] escape(String[] cells) {
        String[] out = new String[cells.length];
        for (int i = 0; i < cells.length; i++) out[i] = escape(cells[i]);
        return out;
    }

    private static String escape(String s) {
        if (s == null) return "";
        boolean needs = s.indexOf(',') >= 0 || s.indexOf('"') >= 0 || s.indexOf('\n') >= 0;
        return needs ? "\"" + s.replace("\"", "\"\"") + "\"" : s;
    }
}
