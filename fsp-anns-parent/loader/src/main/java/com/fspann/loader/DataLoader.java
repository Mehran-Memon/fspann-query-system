package com.fspann.loader;

import java.io.IOException;
import java.util.List;

/**
 * Unified interface for loading feature vectors from various file formats with streaming support.
 */
public interface DataLoader {
    /**
     * Load feature vectors from the specified path in batches.
     * Streaming behavior should be handled internally by the loader.
     *
     * @param path        file path
     * @param batchSize   number of vectors per batch
     * @return list of double arrays representing the next batch of vectors
     * @throws IOException on read errors or unsupported format
     */
    List<double[]> loadData(String path, int batchSize) throws IOException;

    /**
     * Load feature vectors with an expected dimension. Streaming should still apply.
     *
     * @param path          file a path
     * @param expectedDim   number of dimensions expected in each vector
     * @param batchSize     number of vectors per batch
     * @return list of double arrays
     * @throws IOException on read errors
     */
    List<double[]> loadData(String path, int expectedDim, int batchSize) throws IOException;

    /**
     * Load ground-truth nearest neighbor indices from an IVECS file.
     *
     * @param path        file path
     * @param batchSize   number of ground-truth arrays to return
     * @return list of int arrays (each representing nearest neighbor indices)
     * @throws IOException on read errors
     */
    List<int[]> loadGroundTruth(String path, int batchSize) throws IOException;
}
