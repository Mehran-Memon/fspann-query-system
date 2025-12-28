package com.fspann.common;

import java.util.List;
import java.util.BitSet;

/**
 * IndexService: Interface for approximate nearest neighbor indexing.
 *
 * Implementations should support encrypted vector storage and querying.
 */
public interface IndexService {

    /**
     * Insert a vector into the index.
     *
     * @param id       unique identifier
     * @param vector   plaintext vector
     */
    void insert(String id, double[] vector);


    /**
     * Finalize index for search operations.
     * Called after all insertions complete.
     */
    void finalizeForSearch();

    /**
     * Update cached point in the index.
     * Used when a point is re-encrypted or metadata changes.
     *
     * @param point    the updated encrypted point
     */
    default void updateCachedPoint(EncryptedPoint point) {
        // Optional - implementations may override if they cache points
    }

    /**
     * Get the encrypted point buffer (for flushing).
     */
    default EncryptedPointBuffer getPointBuffer() {
        throw new UnsupportedOperationException("getPointBuffer() not implemented");
    }

    /**
     * Code a vector for partition-based indexing.
     * Returns a BitSet array for partition lookup.
     */
    default BitSet[] code(double[] vector) {
        throw new UnsupportedOperationException("code() not implemented");
    }
}