package com.fspann.loader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * CSV format loader: each line is comma-separated doubles.
 * Now maintains offset per path to support batch streaming.
 */
public class CsvLoader implements FormatLoader {

    @Override
    public Iterator<double[]> openVectorIterator(Path file) throws IOException {
        BufferedReader r = Files.newBufferedReader(file);
        return r.lines()
                .map(line -> {
                    try {
                        String[] tok = line.split(",");
                        double[] v = new double[tok.length];
                        for (int i = 0; i < tok.length; i++) {
                            v[i] = Double.parseDouble(tok[i]);
                            if (Double.isNaN(v[i]) || Double.isInfinite(v[i])) {
                                throw new IOException("Invalid value in CSV: " + tok[i]);
                            }
                        }
                        return v;
                    } catch (NumberFormatException e) {
                        throw new UncheckedIOException(new IOException("Malformed CSV line: " + line, e));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .iterator();
    }

    @Override public Iterator<int[]> openIndexIterator(Path file) {
        throw new UnsupportedOperationException("CSV has no indices");
    }
}
