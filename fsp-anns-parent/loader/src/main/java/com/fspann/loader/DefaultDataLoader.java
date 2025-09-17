package com.fspann.loader;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultDataLoader implements Closeable {

    // Sensible default; override with -Dfspann.loader.batchSize=NNN
    private static final int DEFAULT_BATCH_SIZE =
            Integer.getInteger("fspann.loader.batchSize", 10_000);

    // Keep streaming iterators per path so repeated calls don't re-scan files
    private final Map<String, Iterator<double[]>> vectorIts = new ConcurrentHashMap<>();
    private final Map<String, Iterator<int[]>>    indexIts  = new ConcurrentHashMap<>();

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

        Iterator<double[]> it = vectorIts.computeIfAbsent(key, k -> {
            try { return lookup(p).openVectorIterator(p); }
            catch (IOException e) { throw new RuntimeException(e); }
        });

        List<double[]> out = new ArrayList<>(batchSize);
        while (it.hasNext() && out.size() < batchSize) {
            double[] v = it.next();
            if (v != null && v.length == expectedDim) out.add(v);
        }
        if (!it.hasNext()) vectorIts.remove(key); // iterator closes itself at EOF
        return out;
    }

    /** Stream next batch of ground-truth rows. */
    public List<int[]> loadGroundtruth(String path, int batchSize) throws IOException {
        Path p = Path.of(path).toAbsolutePath().normalize();
        String key = p.toString();

        Iterator<int[]> it = indexIts.computeIfAbsent(key, k -> {
            try { return lookup(p).openIndexIterator(p); }
            catch (IOException e) { throw new RuntimeException(e); }
        });

        List<int[]> out = new ArrayList<>(batchSize);
        while (it.hasNext() && out.size() < batchSize) {
            out.add(it.next());
        }
        if (!it.hasNext()) indexIts.remove(key);
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

    @Override public void close() {
        vectorIts.clear();
        indexIts.clear();
    }
}
