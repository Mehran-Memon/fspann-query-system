package com.fspann.common;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * MetadataManager defines the interface for handling metadata associated with encrypted points,
 * including versioning, shard info, and other forward-security-related metadata.
 *
 * This interface is implemented by both:
 * - RocksDBMetadataManager (monolithic)
 * - ShardedMetadataManager (distributed)
 */
public interface MetadataManager extends AutoCloseable {

    // ===== Single Vector Metadata =====

    /**
     * Saves metadata associated with a vector ID.
     */
    void putVectorMetadata(String vectorId, Map<String, String> metadataMap);

    /**
     * Retrieves metadata associated with a vector ID.
     */
    Map<String, String> getVectorMetadata(String vectorId);

    /**
     * Updates existing metadata for a vector ID.
     */
    void updateVectorMetadata(String vectorId, Map<String, String> updates);

    // ===== Batch Metadata Operations =====

    /**
     * Batch update metadata for multiple vectors (critical for performance).
     */
    void batchUpdateVectorMetadata(Map<String, Map<String, String>> updates) throws IOException;

    /**
     * Get all vector IDs stored in metadata.
     */
    List<String> getAllVectorIds();

    // ===== Encrypted Point Persistence =====

    /**
     * Retrieves all stored encrypted points.
     */
    List<EncryptedPoint> getAllEncryptedPoints();

    /**
     * Persists a single encrypted point to disk.
     */
    void saveEncryptedPoint(EncryptedPoint point) throws IOException;

    /**
     * Batch save encrypted points (CRITICAL for 100M scale).
     */
    void saveEncryptedPointsBatch(Collection<EncryptedPoint> points) throws IOException;

    /**
     * Load a single encrypted point by ID.
     */
    EncryptedPoint loadEncryptedPoint(String id) throws IOException, ClassNotFoundException;

    // ===== Lifecycle =====

    /**
     * Flush any pending writes to disk.
     */
    void flush();

    /**
     * Gracefully shuts down (backward compatibility alias for close).
     */
    default void shutdown() {
        try {
            close();
        } catch (Exception e) {
            throw new RuntimeException("Shutdown failed", e);
        }
    }

    /**
     * Close the metadata manager.
     */
    @Override
    void close();

    StorageMetrics getStorageMetrics();

    void saveIndexVersion(int version);

    void printSummary();

    void logStats();

    /** Returns total size of all point data on disk */
    long sizePointsDir();

    /** Counts vectors using a specific key version (for migration tracking) */
    int countWithVersion(int keyVersion) throws IOException;

    /** Returns the version of a specific vector */
    int getVersionOfVector(String id);

    boolean isDeleted(String vectorId);
    long getDeletedTimestamp(String vectorId);
    void hardDeleteVector(String vectorId);

    /**
     * Retrieves a limited sample of vector IDs from a specific shard.
     * Useful for O(1) memory sampling at 100M scale.
     */
    List<String> getIdsFromShard(int shardIndex, int limit);
}