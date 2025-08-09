// loader/src/main/java/com/fspann/loader/DefaultDataLoader.java
package com.fspann.loader;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

/**
 * DefaultDataLoader: maps common ANN benchmark formats to loaders.
 * - Vectors: .csv, .fvecs (optionally .bvecs if you add BvecsLoader)
 * - Groundtruth indices: .csv, .ivecs
 *
 * NOTE: For big datasets (e.g., SIFT1M/1B), prefer StreamingBatchLoader over loadData().
 */
public class DefaultDataLoader {

    public FormatLoader lookup(Path file) {
        String name = file.getFileName().toString().toLowerCase(Locale.ROOT);
        if (name.endsWith(".csv"))   return new CsvLoader();
        if (name.endsWith(".fvecs")) return new FvecsLoader();
        if (name.endsWith(".ivecs")) return new IvecsLoader();
        // If you add a BvecsLoader:
        // if (name.endsWith(".bvecs")) return new BvecsLoader();
        throw new IllegalArgumentException("Unsupported vector/index format: " + name);
    }

    /** Eager load whole file of vectors (.csv or .fvecs). Use StreamingBatchLoader for huge corpora. */
    public List<double[]> loadData(String path, int dim) throws IOException {
        Path p = Path.of(path);
        String name = p.getFileName().toString().toLowerCase(Locale.ROOT);

        // Guard against passing .ivecs (indices) into the vector path by mistake.
        if (name.endsWith(".ivecs")) {
            throw new IllegalArgumentException("loadData() expects vectors (.csv/.fvecs), but got an .ivecs index file: " + name);
        }

        // (Optional) allow .bvecs if you implement BvecsLoader
        if (!(name.endsWith(".csv") || name.endsWith(".fvecs") /*|| name.endsWith(".bvecs")*/)) {
            throw new IllegalArgumentException("Unsupported vector format for loadData(): " + name);
        }

        FormatLoader fl = lookup(p);
        List<double[]> out = new ArrayList<>();
        for (Iterator<double[]> it = fl.openVectorIterator(p); it.hasNext(); ) {
            double[] v = it.next();
            if (v.length == dim) out.add(v); // keep only exact-dim rows
        }
        return out;
    }

    /** Eager load groundtruth indices (.csv or .ivecs). */
    public List<int[]> loadGroundtruth(String path) throws IOException {
        Path p = Path.of(path);
        String name = p.getFileName().toString().toLowerCase(Locale.ROOT);

        if (!(name.endsWith(".csv") || name.endsWith(".ivecs"))) {
            throw new IllegalArgumentException("Unsupported groundtruth format (expected .csv or .ivecs): " + name);
        }

        FormatLoader fl = lookup(p);
        List<int[]> out = new ArrayList<>();
        for (Iterator<int[]> it = fl.openIndexIterator(p); it.hasNext(); ) {
            out.add(it.next());
        }
        return out;
    }
}