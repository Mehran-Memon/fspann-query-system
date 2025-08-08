package com.fspann.loader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultDataLoader implements DataLoader {
    private final Map<String, FormatLoader> registry = new ConcurrentHashMap<>();

    // NEW: persistent iterators so batches stream across calls
    private final Map<Path, Iterator<double[]>> vectorIterators = new ConcurrentHashMap<>();
    private final Map<Path, Iterator<int[]>>    indexIterators  = new ConcurrentHashMap<>();

    public DefaultDataLoader() {
        registry.put("CSV",   new CsvLoader());
        registry.put("FVECS", new FvecsLoader());
        registry.put("IVECS", new IvecsLoader());
    }

    public FormatLoader lookup(Path file) {
        String name = file.getFileName().toString();
        int dot = name.lastIndexOf('.');
        String ext = (dot >= 0 ? name.substring(dot + 1) : "").toUpperCase(Locale.ROOT);
        FormatLoader f = registry.get(ext);
        if (f == null) throw new IllegalArgumentException("No loader for extension: " + ext);
        return f;
    }

    @Override
    public List<double[]> loadData(String path, int expectedDim, int batchSize) throws IOException {
        Path file = Paths.get(path).toAbsolutePath().normalize();

        Iterator<double[]> it = vectorIterators.computeIfAbsent(file, f -> {
            try { return lookup(f).openVectorIterator(f); }
            catch (IOException e) { throw new UncheckedIOException(e); }
        });

        List<double[]> batch = new ArrayList<>(Math.max(1, batchSize));
        while (it.hasNext() && batch.size() < batchSize) {
            batch.add(it.next());
        }
        // Iterator exhausted? remove to allow fresh stream on next call.
        if (batch.isEmpty()) vectorIterators.remove(file);

        if (expectedDim > 0) {
            for (double[] v : batch) {
                if (v.length != expectedDim) {
                    throw new IllegalArgumentException(
                            "Vector dimension mismatch: expected " + expectedDim + ", got " + v.length
                    );
                }
            }
        }
        return batch;
    }

    @Override
    public List<double[]> loadData(String path, int batchSize) throws IOException {
        return loadData(path, 0, batchSize);
    }

    @Override
    public List<int[]> loadGroundTruth(String path, int batchSize) throws IOException {
        Path file = Paths.get(path).toAbsolutePath().normalize();

        Iterator<int[]> it = indexIterators.computeIfAbsent(file, f -> {
            try { return lookup(f).openIndexIterator(f); }
            catch (IOException e) { throw new UncheckedIOException(e); }
        });

        List<int[]> batch = new ArrayList<>(Math.max(1, batchSize));
        while (it.hasNext() && batch.size() < batchSize) {
            batch.add(it.next());
        }
        if (batch.isEmpty()) indexIterators.remove(file);
        return batch;
    }

    /** Optional helper to restart streaming from the beginning for a file. */
    public void reset(String path) {
        Path file = Paths.get(path).toAbsolutePath().normalize();
        vectorIterators.remove(file);
        indexIterators.remove(file);
    }
}
