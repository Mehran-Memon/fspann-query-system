// loader/src/main/java/com/fspann/loader/DefaultDataLoader.java
package com.fspann.loader;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultDataLoader {

    // Stateful per-file cursor so repeated calls consume the file in batches
    private final Map<String, Long> vectorOffsets = new ConcurrentHashMap<>();

    // Default batch size (overridable via -Dfspann.loader.batchSize=N)
    private static final int DEFAULT_BATCH_SIZE =
            Integer.getInteger("fspann.loader.batchSize", 3);

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

    /**
     * Eager-load a small batch of vectors from the given file, remembering
     * how many have already been consumed for that path. Subsequent calls
     * will continue from where the previous call left off.
     */
    public List<double[]> loadData(String path, int dim) throws IOException {
        Path p = Path.of(path).toAbsolutePath().normalize();
        String key = p.toString();

        FormatLoader fl = lookup(p);
        Iterator<double[]> it = fl.openVectorIterator(p);

        // Skip to last offset
        long offset = vectorOffsets.getOrDefault(key, 0L);
        long skipped = 0;
        while (skipped < offset && it.hasNext()) {
            it.next();
            skipped++;
        }
        // If file already exhausted, return empty
        if (skipped < offset && !it.hasNext()) {
            return Collections.emptyList();
        }

        // Read up to DEFAULT_BATCH_SIZE matching-dimension vectors
        List<double[]> out = new ArrayList<>(DEFAULT_BATCH_SIZE);
        int read = 0;
        while (it.hasNext() && read < DEFAULT_BATCH_SIZE) {
            double[] v = it.next();
            if (v != null && v.length == dim) {
                out.add(v);
                read++;
            }
        }

        // Advance the cursor by how many items we actually returned (not lines skipped due to dim mismatch)
        vectorOffsets.put(key, offset + read);
        return out;
    }

    /** Eager load groundtruth indices (CSV or IVECS). No batching/cursor. */
    public List<int[]> loadGroundtruth(String path) throws IOException {
        Path p = Path.of(path).toAbsolutePath().normalize();
        FormatLoader fl = lookup(p);
        List<int[]> out = new ArrayList<>();
        for (Iterator<int[]> it = fl.openIndexIterator(p); it.hasNext(); ) {
            out.add(it.next());
        }
        return out;
    }
}
