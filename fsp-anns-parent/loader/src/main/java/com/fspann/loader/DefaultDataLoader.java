package com.fspann.loader;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultDataLoader implements DataLoader {
    private final Map<String, FormatLoader> registry = new ConcurrentHashMap<>();

    public DefaultDataLoader() {
        registry.put("CSV", new CsvLoader());
        registry.put("FVECS", new FvecsLoader());
        registry.put("IVECS", new IvecsLoader());
    }

    public FormatLoader lookup(Path file) {
        String name = file.getFileName().toString();
        int dot = name.lastIndexOf('.');
        String ext = (dot >= 0 ? name.substring(dot + 1) : "").toUpperCase();
        FormatLoader f = registry.get(ext);
        if (f == null) {
            throw new IllegalArgumentException("No loader for extension: " + ext);
        }
        return f;
    }

    @Override
    public List<double[]> loadData(String path, int expectedDim, int batchSize) throws IOException {
        Path file = Paths.get(path);
        List<double[]> vectors = lookup(file).loadVectors(file, batchSize);
        if (expectedDim > 0) {
            for (double[] vector : vectors) {
                if (vector.length != expectedDim) {
                    throw new IllegalArgumentException(
                            "Vector dimension mismatch: expected " + expectedDim + ", got " + vector.length
                    );
                }
            }
        }
        return vectors;
    }

    @Override
    public List<double[]> loadData(String path, int batchSize) throws IOException {
        return loadData(path, 0, batchSize);
    }

    @Override
    public List<int[]> loadGroundTruth(String path, int batchSize) throws IOException {
        Path file = Paths.get(path);
        return lookup(file).loadIndices(file, batchSize);
    }
}