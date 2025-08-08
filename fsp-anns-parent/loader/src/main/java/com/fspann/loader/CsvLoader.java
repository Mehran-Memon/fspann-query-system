package com.fspann.loader;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Objects;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * CSV format loader: each line is comma-separated doubles.
 * Now maintains offset per path to support batch streaming.
 */
public class CsvLoader implements FormatLoader {
    private static final Logger logger = Logger.getLogger(CsvLoader.class.getName());

    @Override
    public Iterator<double[]> openVectorIterator(Path file) throws IOException {
        BufferedReader reader = Files.newBufferedReader(file);
        return reader.lines()
                .map(this::parseLine)
                .filter(Objects::nonNull)
                .iterator();
    }

    private double[] parseLine(String raw) {
        String line = raw.trim();
        if (line.isEmpty()) {
            return null;
        }
        String[] tokens = line.split(",");
        double[] vector = new double[tokens.length];
        for (int i = 0; i < tokens.length; i++) {
            String tok = tokens[i].trim();
            try {
                double val = Double.parseDouble(tok);
                if (Double.isNaN(val) || Double.isInfinite(val)) {
                    logger.warning(() -> "Invalid numeric value '" + tok + "' in CSV line: " + line);
                    return null;
                }
                vector[i] = val;
            } catch (NumberFormatException nfe) {
                logger.warning(() -> "Skipping malformed CSV line: " + line);
                return null;
            }
        }
        return vector;
    }

    @Override
    public Iterator<int[]> openIndexIterator(Path file) {
        throw new UnsupportedOperationException("CSV has no indices");
    }
}
