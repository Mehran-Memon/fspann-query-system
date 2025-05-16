package com.fspann.loader;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * CSV format loader: each line is comma-separated doubles.
 */
public class CsvLoader implements FormatLoader {
    @Override
    public List<double[]> loadVectors(String path, int batchSize) throws IOException {
        List<double[]> data = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(path), StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] tokens = line.split(",");
                double[] vec = new double[tokens.length];
                for (int i = 0; i < tokens.length; i++) {
                    vec[i] = Double.parseDouble(tokens[i]);
                }
                data.add(vec);
            }
        }
        return data;
    }

    @Override
    public List<int[]> loadIndices(String path, int batchSize) {
        throw new UnsupportedOperationException("CSV does not support loading indices");
    }
}