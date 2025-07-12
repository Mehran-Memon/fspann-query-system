package com.fspann.loader;

import java.io.IOException;
import java.util.List;

/**
 * Unified interface for loading feature vectors from various file formats.
 */
public interface DataLoader {
    /**
     * Load feature vectors from the specified path.
     * Supports CSV, JSON, FVECS, IVECS, NPZ, and Parquet formats.
     *
     * @param path      file path
     * @param batchSize batch size for streaming loaders
     * @return list of double arrays representing vectors
     * @throws IOException on read errors or unsupported format
     */
    List<double[]> loadData(String path, int batchSize) throws IOException;

    List<double[]> loadData(String path, int expectedDim, int batchSize) throws IOException;

    /**
     * Load ground-truth nearest neighbor indices from an .ivecs file.
     *
     * @param path      .ivecs file path
     * @param batchSize batch size for streaming
     * @return list of int arrays (neighbor lists)
     * @throws IOException on read errors
     */
    List<int[]> loadGroundTruth(String path, int batchSize) throws IOException;
}
