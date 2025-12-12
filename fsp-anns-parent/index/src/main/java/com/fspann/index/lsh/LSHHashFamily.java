package com.fspann.index.lsh;

/**
 * Interface for Locality-Sensitive Hashing families.
 *
 * Defines contract for LSH implementations supporting multi-table indexing
 * with adaptive bucket sizing and collision probability estimation.
 *
 * @author FSP-ANNS Project
 * @version 1.0
 */
public interface LSHHashFamily {

    /**
     * Initialize the LSH hash family.
     *
     * Sets up internal data structures for L independent hash tables,
     * each with K hash functions for d-dimensional vectors.
     *
     * @param dimension         Vector dimensionality (d)
     * @param numTables         Number of independent hash tables (L)
     * @param numFunctions      Number of hash functions per table (K)
     * @param numBuckets        Initial number of buckets per table (B)
     *                          Note: Actual bucket space may be larger
     */
    void init(int dimension, int numTables, int numFunctions, int numBuckets);

    /**
     * Hash a single vector using a specific table and function.
     *
     * Computes the hash value for a vector using the specified hash function
     * from the specified table. Multiple calls to this method with the same
     * vector should return the same hash value (deterministic).
     *
     * @param vector            Input vector (d-dimensional array)
     * @param tableId           Table index [0, numTables)
     * @param functionId        Function index [0, numFunctions)
     *
     * @return                  Bucket index (hash value) in range [0, numBuckets)
     *
     * @throws IllegalArgumentException if indices out of bounds
     */
    int hash(double[] vector, int tableId, int functionId);

    /**
     * Hash a batch of vectors for a specific table.
     *
     * Computes all K hash values for each vector in a batch.
     * This is a convenience method for batch processing, equivalent to calling
     * hash() multiple times but potentially more efficient.
     *
     * @param vectors           2D array of vectors (n × d), where n is number
     *                          of vectors and d is dimension
     * @param tableId           Table index [0, numTables)
     *
     * @return                  2D array of hash values (n × K)
     *                          where result[i][k] = hash(vectors[i], tableId, k)
     */
    int[][] hashBatch(double[][] vectors, int tableId);

    /**
     * Estimate collision probability for two vectors.
     *
     * Returns the probability that hash(u) == hash(v) in a single table.
     * Used for:
     * - Adaptive probing (deciding when to expand/contract search)
     * - Parameter tuning (predicting expected candidate set size)
     * - Theoretical analysis
     *
     * @param distance          Euclidean distance between vectors u and v
     * @param tableId           Table index (may affect computation)
     *
     * @return                  Probability in range [0.0, 1.0]
     *                          - 1.0 = certain collision (distance ≈ 0)
     *                          - 0.0 = no collision (distance >> bucketWidth)
     */
    double collisionProbability(double distance, int tableId);

    /**
     * Serialize hash functions for persistence.
     *
     * Converts all internal hash function parameters and state to a byte array
     * suitable for storage or transmission. The serialized format should be
     * sufficient to recreate an identical hash family via deserialize().
     *
     * @return                  Byte array containing serialized hash family
     * @throws RuntimeException if serialization fails
     */
    byte[] serialize();

    /**
     * Deserialize hash functions from persisted state.
     *
     * Reconstructs the hash family from a previously serialized byte array.
     * After calling this method, the hash family should be in the same state
     * as when it was serialized.
     *
     * @param data              Byte array from a prior serialize() call
     * @throws RuntimeException if deserialization fails
     */
    void deserialize(byte[] data);

    /**
     * Get configuration information.
     *
     * @return String representation of hash family configuration
     *         (useful for logging and debugging)
     */
    String getConfiguration();
}