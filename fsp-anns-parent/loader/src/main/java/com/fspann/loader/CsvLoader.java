package com.fspann.loader;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * CSV format loader: each line is comma-separated doubles.
 * Now maintains offset per path to support batch streaming.
 */
public class CsvLoader implements FormatLoader {
    private final Map<String, Integer> fileOffsets = new ConcurrentHashMap<>();

    @Override
    public List<double[]> loadVectors(String path, int batchSize) throws IOException {
        int offset = fileOffsets.getOrDefault(path, 0);
        List<double[]> batch = new ArrayList<>(batchSize);

        try (BufferedReader reader = Files.newBufferedReader(Paths.get(path), StandardCharsets.UTF_8)) {
            int currentLine = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                if (currentLine++ < offset) continue;

                String[] tokens = line.split(",");
                double[] vec = new double[tokens.length];
                for (int i = 0; i < tokens.length; i++) {
                    try {
                        vec[i] = Double.parseDouble(tokens[i].trim());
                    } catch (NumberFormatException e) {
                        throw new IOException("Invalid number at line: " + line, e);
                    }
                }

                batch.add(vec);
                if (batch.size() >= batchSize) {
                    break;
                }
            }

            fileOffsets.put(path, offset + batch.size());
        }

        return batch;
    }

    @Override
    public List<int[]> loadIndices(String path, int batchSize) {
        throw new UnsupportedOperationException("CSV does not support loading indices");
    }
}
