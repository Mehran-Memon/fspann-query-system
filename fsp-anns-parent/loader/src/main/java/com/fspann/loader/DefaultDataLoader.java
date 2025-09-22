package com.fspann.loader;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultDataLoader implements Closeable {

    // Default batch size; can be overridden with -Dfspann.loader.batchSize=NNN
    private static final int DEFAULT_BATCH_SIZE =
            Integer.getInteger("fspann.loader.batchSize", 10_000);

    // Per-path live iterators (streaming)
    private final Map<String, Iterator<double[]>> vectorIts = new ConcurrentHashMap<>();
    private final Map<String, Iterator<int[]>>    indexIts  = new ConcurrentHashMap<>();

    // Paths that have reached EOF (so we don't reopen and rescan)
    private final Set<String> exhaustedVectors = ConcurrentHashMap.newKeySet();
    private final Set<String> exhaustedIndices = ConcurrentHashMap.newKeySet();

    public FormatLoader lookup(Path file) {
        String name = file.getFileName().toString().toLowerCase(Locale.ROOT);
        if (name.endsWith(".csv"))   return new CsvLoader();
        if (name.endsWith(".fvecs")) return new FvecsLoader();
        if (name.endsWith(".bvecs") || name.endsWith(".bvec") || name.endsWith(".siftbin")) {
            return new BvecsLoader();
        }
        if (name.endsWith(".ivecs")) return new IvecsLoader();
        throw new IllegalArgumentException("Unsupported vector/index format: " + name);
    }

    /** Stream next batch of vectors filtered by expected dimension. */
    public List<double[]> loadData(String path, int expectedDim) throws IOException {
        return loadData(path, expectedDim, DEFAULT_BATCH_SIZE);
    }

    /** Stream next batch of vectors filtered by expected dimension. */
    public List<double[]> loadData(String path, int expectedDim, int batchSize) throws IOException {
        Path p = Path.of(path).toAbsolutePath().normalize();
        String key = p.toString();

        // If we've already reached EOF on this path, stay exhausted.
        if (exhaustedVectors.contains(key)) {
            return Collections.emptyList();
        }

        Iterator<double[]> it = vectorIts.computeIfAbsent(key, k -> {
            try { return lookup(p).openVectorIterator(p); }
            catch (IOException e) { throw new RuntimeException(e); }
        });

        List<double[]> out = new ArrayList<>(batchSize);
        while (it.hasNext() && out.size() < batchSize) {
            double[] v = it.next();
            if (v != null && v.length == expectedDim) out.add(v);
        }

        // If iterator is finished, mark the path as exhausted and drop the iterator.
        if (!it.hasNext()) {
            exhaustedVectors.add(key);
            vectorIts.remove(key);
        }
        return out;
    }

    /** Stream next batch of ground-truth rows. */
    public List<int[]> loadGroundtruth(String path, int batchSize) throws IOException {
        Path p = Path.of(path).toAbsolutePath().normalize();
        String key = p.toString();

        if (exhaustedIndices.contains(key)) {
            return Collections.emptyList();
        }

        Iterator<int[]> it = indexIts.computeIfAbsent(key, k -> {
            try { return lookup(p).openIndexIterator(p); }
            catch (IOException e) { throw new RuntimeException(e); }
        });

        List<int[]> out = new ArrayList<>(batchSize);
        while (it.hasNext() && out.size() < batchSize) {
            out.add(it.next());
        }

        if (!it.hasNext()) {
            exhaustedIndices.add(key);
            indexIts.remove(key);
        }
        return out;
    }

    /** Convenience: read all ground-truth rows (for medium-sized GT files). */
    public List<int[]> loadGroundtruthAll(String path) throws IOException {
        List<int[]> all = new ArrayList<>();
        List<int[]> batch;
        while (!(batch = loadGroundtruth(path, DEFAULT_BATCH_SIZE)).isEmpty()) {
            all.addAll(batch);
        }
        return all;
    }

    @Override
    public void close() {
        vectorIts.clear();
        indexIts.clear();
        exhaustedVectors.clear();
        exhaustedIndices.clear();
    }
}
