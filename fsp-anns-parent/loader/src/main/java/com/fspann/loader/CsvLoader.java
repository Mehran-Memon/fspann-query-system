package com.fspann.loader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * CSV format loader: each line is comma-separated doubles.
 * Streaming iterator that closes its reader on EOF.
 */
public class CsvLoader implements FormatLoader {
    private static final Logger logger = LoggerFactory.getLogger(CsvLoader.class);

    @Override
    public Iterator<double[]> openVectorIterator(Path file) throws IOException {
        final BufferedReader reader = Files.newBufferedReader(file);
        return new Iterator<>() {
            String nextLine = advance();

            private String advance() {
                try {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        line = line.trim();
                        if (!line.isEmpty()) return line;
                    }
                    close();
                    return null;
                } catch (IOException e) {
                    close();
                    throw new RuntimeException("Failed reading CSV: " + file, e);
                }
            }

            private void close() {
                try { reader.close(); } catch (IOException ignored) {}
            }

            @Override public boolean hasNext() { return nextLine != null; }

            @Override
            public double[] next() {
                if (nextLine == null) throw new NoSuchElementException();
                double[] vec = parseLine(nextLine);
                nextLine = advance();
                return vec;
            }
        };
    }

    private double[] parseLine(String line) {
        String[] tokens = line.split(",");
        double[] vector = new double[tokens.length];
        for (int i = 0; i < tokens.length; i++) {
            String tok = tokens[i].trim();
            try {
                double val = Double.parseDouble(tok);
                if (Double.isNaN(val) || Double.isInfinite(val)) {
                    logger.warn("Invalid numeric value '{}' in CSV line: {}", tok, line);
                    throw new NumberFormatException("NaN/Inf");
                }
                vector[i] = val;
            } catch (NumberFormatException nfe) {
                logger.warn("Skipping malformed CSV line: {}", line);
                // If you'd rather drop bad lines silently, return null and filter; here we fail-fast
                throw nfe;
            }
        }
        return vector;
    }

    @Override
    public Iterator<int[]> openIndexIterator(Path file) {
        throw new UnsupportedOperationException("CSV has no indices");
    }
}
