package com.fspann.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.fasterxml.jackson.core.type.TypeReference;

/**
 * Manager for persisting structured metadata, including vector-specific data (e.g., shard ID, version).
 */
public class MetadataManager {
    private static final Logger logger = LoggerFactory.getLogger(MetadataManager.class);
    private final Map<String, Map<String, String>> metadata = new ConcurrentHashMap<>(); // vectorId -> {shardId, version}
    private String currentPath; // Track the last save/load path

    public MetadataManager() {
        // Initialize with empty map for thread safety
    }

    /**
     * Loads metadata from a file.
     * @param path Path to the metadata file.
     * @throws MetadataException if loading fails.
     */
    public void load(String path) throws MetadataException {
        try {
            TypeReference<MetadataDTO> typeRef = new TypeReference<MetadataDTO>() {};
            MetadataDTO loaded = (MetadataDTO) PersistenceUtils.loadObject(path, typeRef);
            logger.debug("Loaded metadata DTO: {}", loaded != null ? loaded.getMetadata() : "null");
            metadata.clear();
            if (loaded != null && loaded.getMetadata() != null) {
                metadata.putAll(loaded.getMetadata());
            }
            this.currentPath = path;
            logger.info("Loaded metadata from {}", path);
        } catch (IOException | ClassNotFoundException e) {
            logger.warn("Failed to load metadata from {}, starting fresh", path, e);
            metadata.clear();
            this.currentPath = null;
            throw new MetadataException("Failed to load metadata from " + path, e);
        }
    }

    /**
     * Saves metadata to a file.
     * @param path Path to save the metadata file.
     * @throws MetadataException if saving fails.
     */
    public void save(String path) throws MetadataException {
        try {
            MetadataDTO dto = new MetadataDTO(new HashMap<>(metadata));
            logger.debug("Saving metadata: {}", dto.getMetadata());
            PersistenceUtils.saveObject(dto, path);
            this.currentPath = path;
            logger.info("Saved metadata to {}", path);
        } catch (IOException e) {
            logger.error("Failed to save metadata to {}", path, e);
            throw new MetadataException("Failed to save metadata to " + path, e);
        }
    }

    /**
     * Adds or updates metadata for a vector using a map and persists it.
     * @param vectorId The ID of the vector.
     * @param metadata A map containing metadata key-value pairs (e.g., "version", "shardId").
     */
    public synchronized void updateVectorMetadata(String vectorId, Map<String, String> metadata) {
        if (vectorId == null || metadata == null) {
            throw new IllegalArgumentException("vectorId and metadata cannot be null");
        }
        // Compute if absent on the class field metadata
        Map<String, String> vectorMetadata = this.metadata.computeIfAbsent(vectorId, k -> new HashMap<String, String>());
        vectorMetadata.putAll(metadata); // Merge the provided metadata
        logger.debug("Updated metadata for vectorId={} with {}", vectorId, metadata);
        // Persist if a path is set
        if (currentPath != null) {
            try {
                save(currentPath);
            } catch (MetadataException e) {
                logger.warn("Failed to persist metadata after update for vectorId {}", vectorId, e);
            }
        }
    }

    /**
     * Adds or updates metadata for a vector.
     * @param vectorId The ID of the vector.
     * @param shardId The shard ID.
     * @param version The key version.
     */
    public synchronized void putVectorMetadata(String vectorId, String shardId, String version) {
        if (vectorId == null || shardId == null || version == null) {
            throw new IllegalArgumentException("vectorId, shardId, and version cannot be null");
        }
        metadata.computeIfAbsent(vectorId, k -> new HashMap<String, String>()).put("shardId", shardId);
        metadata.computeIfAbsent(vectorId, k -> new HashMap<String, String>()).put("version", version);
        logger.debug("Updated metadata for vectorId={} with shardId={} and version={}", vectorId, shardId, version);
        // Persist if a path is set
        if (currentPath != null) {
            try {
                save(currentPath);
            } catch (MetadataException e) {
                logger.warn("Failed to persist metadata after update for vectorId {}", vectorId, e);
            }
        }
    }

    /**
     * Retrieves metadata for a vector.
     * @param vectorId The ID of the vector.
     * @return A map containing shardId and version, or empty map if not found.
     */
    public Map<String, String> getVectorMetadata(String vectorId) {
        Map<String, String> meta = metadata.getOrDefault(vectorId, new HashMap<>());
        if (meta.isEmpty()) {
            logger.warn("No metadata found for vectorId {}, using default", vectorId);
        }
        return Collections.unmodifiableMap(meta);
    }

    /**
     * Retrieves all metadata.
     * @return An unmodifiable view of all metadata.
     */
    public Map<String, Map<String, String>> all() {
        return Collections.unmodifiableMap(new HashMap<>(metadata));
    }

    /**
     * Custom exception for metadata operations.
     */
    public static class MetadataException extends Exception {
        public MetadataException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    // DTO for serialization
    private static class MetadataDTO implements java.io.Serializable {
        private final Map<String, Map<String, String>> metadata;

        public MetadataDTO(Map<String, Map<String, String>> metadata) {
            this.metadata = new HashMap<>(metadata);
        }

        public Map<String, Map<String, String>> getMetadata() {
            return new HashMap<>(metadata);
        }
    }
}