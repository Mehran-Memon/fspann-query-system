package com.fspann.loader;

import java.io.IOException;
import java.util.List;

/**
 * Strategy interface for file-format–specific loading logic.
 * Supports streaming via offset-aware batch loading.
 */
public interface FormatLoader {
    /**
     * Loads a batch of vectors from a given file.
     *
     * @param path       file a path
     * @param batchSize  number of vectors to load in this batch
     * @return a list of double[] vectors
     * @throws IOException if reading fails or the file is malformed
     */
    List<double[]> loadVectors(String path, int batchSize) throws IOException;

    /**
     * Loads a batch of integer index arrays (e.g., ground-truth neighbors).
     *
     * @param path       file path (typically IVECS format)
     * @param batchSize  number of index arrays to load
     * @return list of int arrays
     * @throws IOException if reading fails
     */
    List<int[]> loadIndices(String path, int batchSize) throws IOException;
}
