package com.fspann.loader;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Unified interface for file‐format streaming.
 * We provide default batch methods on top of the two iterators.
 */
public interface FormatLoader {
    /** Stream all vectors one at a time. */
    Iterator<double[]> openVectorIterator(Path file) throws IOException;

    /** Stream all ground‐truth indices one array at a time. */
    Iterator<int[]> openIndexIterator(Path file) throws IOException;

    /** Default “batch” wrapper over the vector iterator. */
    default List<double[]> loadVectors(Path file, int batchSize) throws IOException {
        Iterator<double[]> it = openVectorIterator(file);
        List<double[]> batch = new ArrayList<>(batchSize);
        while (it.hasNext() && batch.size() < batchSize) {
            batch.add(it.next());
        }
        return batch;
    }

    /** Default “batch” wrapper over the index iterator. */
    default List<int[]> loadIndices(Path file, int batchSize) throws IOException {
        Iterator<int[]> it = openIndexIterator(file);
        List<int[]> batch = new ArrayList<>(batchSize);
        while (it.hasNext() && batch.size() < batchSize) {
            batch.add(it.next());
        }
        return batch;
    }
}
