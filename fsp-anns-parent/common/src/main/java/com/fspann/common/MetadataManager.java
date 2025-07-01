package com.fspann.common;

import com.fasterxml.jackson.core.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class MetadataManager {
    private static final Logger logger = LoggerFactory.getLogger(MetadataManager.class);
    private static final String BASE_DIR = "metadata/points";
    private final Map<String, Map<String, String>> metadata = new ConcurrentHashMap<>();
    private final Set<String> encryptedPoints = ConcurrentHashMap.newKeySet();
    private String currentPath;
    private boolean deferSave = false;


    public MetadataManager() {
        ensureBaseDirExists();
    }

    private void ensureBaseDirExists() {
        try {
            Files.createDirectories(Paths.get(BASE_DIR));
        } catch (IOException e) {
            logger.error("Failed to create base directory: {}", BASE_DIR, e);
        }
    }

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

    public void save(String path) throws MetadataException {
        try {
            MetadataDTO dto = new MetadataDTO(new HashMap<>(metadata));
            logger.debug("Saving metadata: {}", dto.getMetadata());
            PersistenceUtils.saveObject(dto, path);
            this.currentPath = path;
        } catch (IOException e) {
            logger.error("Failed to save metadata to {}", path, e);
            throw new MetadataException("Failed to save metadata to " + path, e);
        }
    }

    public synchronized void updateVectorMetadata(String vectorId, Map<String, String> metadata) {
        if (vectorId == null || metadata == null) {
            throw new IllegalArgumentException("vectorId and metadata cannot be null");
        }
        Map<String, String> vectorMetadata = this.metadata.computeIfAbsent(vectorId, k -> new HashMap<>());
        vectorMetadata.putAll(metadata);
        logger.debug("Updated metadata for vectorId={} with {}", vectorId, metadata);
        if (currentPath != null) {
            try {
                save(currentPath);
            } catch (MetadataException e) {
                logger.warn("Failed to persist metadata after update for vectorId {}", vectorId, e);
            }
        }
    }

    public synchronized void putVectorMetadata(String vectorId, String shardId, String version) {
        if (vectorId == null || shardId == null || version == null) {
            throw new IllegalArgumentException("vectorId, shardId, and version cannot be null");
        }
        metadata.computeIfAbsent(vectorId, k -> new HashMap<>()).put("shardId", shardId);
        metadata.computeIfAbsent(vectorId, k -> new HashMap<>()).put("version", version);
        logger.debug("Updated metadata for vectorId={} with shardId={} and version={}", vectorId, shardId, version);
        if (currentPath != null && !deferSave) {
            try {
                save(currentPath);
            } catch (MetadataException e) {
                logger.warn("Failed to persist metadata after update for vectorId {}", vectorId, e);
            }
        }
    }

    public Map<String, String> getVectorMetadata(String vectorId) {
        Map<String, String> meta = metadata.getOrDefault(vectorId, new HashMap<>());
        if (meta.isEmpty()) {
            logger.warn("No metadata found for vectorId {}, using default", vectorId);
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(meta);
    }

    public Map<String, Map<String, String>> all() {
        return Collections.unmodifiableMap(new HashMap<>(metadata));
    }

    public List<EncryptedPoint> getAllEncryptedPoints() {
        List<EncryptedPoint> points = new ArrayList<>();
        for (String vectorId : metadata.keySet()) {
            EncryptedPoint pt = loadEncryptedPoint(vectorId);
            if (pt != null) {
                points.add(pt);
            } else {
                logger.warn("Skipping missing or corrupted point: {}", vectorId);
            }
        }
        return points;
    }

    public void saveEncryptedPoint(EncryptedPoint pt) {
        try {
            String versionStr = getVectorMetadata(pt.getId()).get("version");
            if (versionStr == null) versionStr = "v_unknown";

            Path versionDir = Paths.get(BASE_DIR).resolve("v" + versionStr);
            Files.createDirectories(versionDir);

            Path filePath = versionDir.resolve(pt.getId() + ".point");
            PersistenceUtils.saveObject(pt, filePath.toString());
        } catch (IOException e) {
            logger.error("Failed to persist updated point: {}", pt.getId(), e);
        }
    }

    public EncryptedPoint loadEncryptedPoint(String id) {
        try {
            Path basePath = Paths.get(BASE_DIR);
            try (Stream<Path> paths = Files.walk(basePath)) {
                Optional<Path> found = paths
                        .filter(p -> p.getFileName().toString().equals(id + ".point"))
                        .findFirst();

                if (found.isPresent()) {
                    return (EncryptedPoint) PersistenceUtils.loadObject(
                            found.get().toString(),
                            new TypeReference<EncryptedPoint>() {}
                    );
                } else {
                    logger.warn("Point {} not found in any version folder", id);
                    return null;
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to load encrypted point for ID: {}", id, e);
            return null;
        }
    }

    public static class MetadataException extends Exception {
        public MetadataException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static class MetadataDTO implements java.io.Serializable {
        private final Map<String, Map<String, String>> metadata;

        public MetadataDTO(Map<String, Map<String, String>> metadata) {
            this.metadata = new HashMap<>(metadata);
        }

        public Map<String, Map<String, String>> getMetadata() {
            return new HashMap<>(metadata);
        }
    }

    public void setDeferSave(boolean deferSave) {
        this.deferSave = deferSave;
    }

}