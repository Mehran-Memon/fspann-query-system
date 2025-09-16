package com.fspann.common;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * MetadataManager defines the interface for handling metadata associated with encrypted points,
 * including versioning, shard info, and other forward-security-related metadata.
 */
public interface MetadataManager {

    /**
     * Saves metadata associated with a vector ID.
     *
     * @param vectorId     the ID of the vector
     * @param metadataMap  key-value metadata entries
     */
    void putVectorMetadata(String vectorId, Map<String, String> metadataMap);

    /**
     * Retrieves metadata associated with a vector ID.
     *
     * @param vectorId the ID of the vector
     * @return metadata key-value pairs, or empty map if not found
     */
    Map<String, String> getVectorMetadata(String vectorId);

    /**
     * Updates existing metadata for a vector ID.
     *
     * @param vectorId the ID of the vector
     * @param updates  metadata fields to update
     */
    void updateVectorMetadata(String vectorId, Map<String, String> updates);

    /**
     * Retrieves all stored encrypted points.
     * Useful for re-indexing or rotation.
     *
     * @return a list of all encrypted points stored in metadata
     */
    List<EncryptedPoint> getAllEncryptedPoints();

    /**
     * Persists a single encrypted point to disk or backing store.
     *
     * @param point the encrypted point to save
     * @throws IOException if writing fails
     */
    void saveEncryptedPoint(EncryptedPoint point) throws IOException;

    /**
     * Gracefully shuts down any persistent store or resources.
     */
    void shutdown();
}
