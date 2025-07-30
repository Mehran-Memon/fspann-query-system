package com.fspann.common;

import java.util.List;
import java.util.Set;

/**
 * Abstraction for index operations: insert, delete, lookup, and dirty marking.
 * Extended for dimension-aware indexing.
 */
public interface IndexService {

    /**
     * Inserts a fully encrypted point into the appropriate index (based on its dimension).
     */
    void insert(EncryptedPoint point);

    /**
     * Inserts a raw vector and determines its dimension automatically.
     */
    void insert(String id, double[] vector);

    /**
     * Deletes a point by its ID (across all dimensions).
     */
    void delete(String id);

    /**
     * Lookup using a query token (dimension inferred from token).
     */
    List<EncryptedPoint> lookup(QueryToken token);

    /**
     * Mark a specific shard as dirty for key rotation within its dimension.
     */
    void markDirty(int shardId);

    /**
     * Total number of indexed points (across all dimensions).
     */
    int getIndexedVectorCount();

    /**
     * List all registered dimensions being tracked.
     */
    Set<Integer> getRegisteredDimensions();

    /**
     * Get the number of indexed vectors for a specific dimension.
     */
    int getVectorCountForDimension(int dimension);

    EncryptedPoint getEncryptedPoint(String id);

    EncryptedPointBuffer getPointBuffer();

    int getShardIdForVector(double[] vector);

    void clearCache();
}
