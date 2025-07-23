package com.fspann.loader;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultDataLoader implements DataLoader {
    private final Map<String,FormatLoader> registry = new ConcurrentHashMap<>();

    public DefaultDataLoader() {
        registry.put("CSV",   new CsvLoader());
        registry.put("FVECS", new FvecsLoader());
        registry.put("IVECS", new IvecsLoader());
    }

    /** Publicly expose the same logic you had there before. */
    public FormatLoader lookup(Path file) {
        String name = file.getFileName().toString();
        int dot = name.lastIndexOf('.');
        String ext = (dot>=0 ? name.substring(dot+1) : "").toUpperCase();
        FormatLoader f = registry.get(ext);
        if (f==null) throw new IllegalArgumentException("No loader for “" + ext + "”");
        return f;
    }

    @Override
    public List<double[]> loadData(String path, int expectedDim, int batchSize) throws IOException {
        Path file = Paths.get(path);
        return lookup(file).loadVectors(file, batchSize);
    }

    @Override
    public List<double[]> loadData(String path, int batchSize) throws IOException {
        return loadData(path, 0, batchSize);
    }

    private FormatLoader getLoader(Path file) {
        String name = file.getFileName().toString();
        int dot = name.lastIndexOf('.');
        String ext = (dot >= 0 ? name.substring(dot + 1) : "").toUpperCase();
        FormatLoader fl = registry.get(ext);
        if (fl == null) {
            throw new UnsupportedOperationException("No loader for extension: " + ext);
        }
        return fl;
    }

    @Override
    public List<int[]> loadGroundTruth(String path, int batchSize) throws IOException {
        Path file = Paths.get(path);
        return lookup(file).loadIndices(file, batchSize);
    }
}
