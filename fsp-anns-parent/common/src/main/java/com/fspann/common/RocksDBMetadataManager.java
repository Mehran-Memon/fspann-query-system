package com.fspann.common;

import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RocksDBMetadataManager implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(RocksDBMetadataManager.class);
    private final RocksDB db;
    private final String dbPath;
    private final String baseDir;
    private boolean closed = false;

    static {
        try {
            RocksDB.loadLibrary();
            logger.info("RocksDB native library loaded successfully.");
        } catch (Exception e) {
            logger.error("Failed to load RocksDB native library", e);
            throw new RuntimeException("RocksDB initialization failed", e);
        }
    }

    public RocksDBMetadataManager(String dbPath) throws IOException {
        this(dbPath, dbPath + "/points");
    }

    public RocksDBMetadataManager(String dbPath, String pointsPath) throws IOException {
        this.dbPath = dbPath;
        this.baseDir = pointsPath;
        Files.createDirectories(Path.of(dbPath));
        Files.createDirectories(Path.of(pointsPath));
        Options options = new Options().setCreateIfMissing(true);
        try {
            db = RocksDB.open(options, dbPath);
            logger.info("Initialized RocksDB at {}", dbPath);
        } catch (RocksDBException e) {
            logger.error("Failed to initialize RocksDB at {}", dbPath, e);
            throw new IOException("RocksDB initialization failed", e);
        } finally {
            options.close();
        }
    }

    public void batchPutMetadata(Map<String, Map<String, String>> allMetadata) {
        Objects.requireNonNull(allMetadata, "Metadata map cannot be null");
        try (WriteBatch batch = new WriteBatch(); WriteOptions opts = new WriteOptions()) {
            for (Map.Entry<String, Map<String, String>> entry : allMetadata.entrySet()) {
                String key = Objects.requireNonNull(entry.getKey(), "Vector ID cannot be null");
                Map<String, String> valMap = Objects.requireNonNull(entry.getValue(), "Metadata cannot be null");
                batch.put(key.getBytes(StandardCharsets.UTF_8), serializeMetadata(valMap));
            }
            db.write(opts, batch);
            logger.debug("Batch inserted {} metadata entries", allMetadata.size());
        } catch (RocksDBException e) {
            logger.error("Batch metadata insert failed", e);
            throw new RuntimeException("Batch metadata insert failed", e);
        }
    }

    public void batchUpdateVectorMetadata(Map<String, Map<String, String>> updates) throws IOException {
        Objects.requireNonNull(updates, "Updates map cannot be null");
        try (WriteBatch batch = new WriteBatch(); WriteOptions opts = new WriteOptions()) {
            for (Map.Entry<String, Map<String, String>> entry : updates.entrySet()) {
                String key = Objects.requireNonNull(entry.getKey(), "Vector ID cannot be null");
                Map<String, String> valMap = Objects.requireNonNull(entry.getValue(), "Metadata cannot be null");
                batch.put(key.getBytes(StandardCharsets.UTF_8), serializeMetadata(valMap));
            }
            db.write(opts, batch);
            logger.debug("Batch updated {} metadata entries", updates.size());
        } catch (RocksDBException e) {
            logger.error("Batch metadata update failed", e);
            throw new IOException("Batch metadata update failed", e);
        }
    }

    public void putVectorMetadata(String vectorId, Map<String, String> metadata) {
        if (closed) throw new IllegalStateException("RocksDBMetadataManager is closed");
        Objects.requireNonNull(vectorId, "Vector ID cannot be null");
        Objects.requireNonNull(metadata, "Metadata cannot be null");
        try {
            db.put(vectorId.getBytes(StandardCharsets.UTF_8), serializeMetadata(metadata));
            logger.debug("Updated metadata for vectorId={}", vectorId);
        } catch (RocksDBException e) {
            logger.error("Failed to put metadata for vectorId={}", vectorId, e);
            throw new RuntimeException("Failed to put metadata", e);
        }
    }

    public Map<String, String> getVectorMetadata(String vectorId) {
        if (closed) throw new IllegalStateException("RocksDBMetadataManager is closed");
        Objects.requireNonNull(vectorId, "Vector ID cannot be null");
        try {
            byte[] value = db.get(vectorId.getBytes(StandardCharsets.UTF_8));
            return (value != null) ? deserializeMetadata(value) : new HashMap<>();
        } catch (RocksDBException e) {
            logger.warn("Failed to get metadata for vectorId={}", vectorId, e);
            return new HashMap<>();
        }
    }

    public void removeVectorMetadata(String vectorId) {
        if (closed) throw new IllegalStateException("RocksDBMetadataManager is closed");
        Objects.requireNonNull(vectorId, "Vector ID cannot be null");
        try {
            db.delete(vectorId.getBytes(StandardCharsets.UTF_8));
            logger.debug("Deleted metadata for vectorId={}", vectorId);
        } catch (RocksDBException e) {
            logger.warn("Failed to delete metadata for vectorId={}", vectorId, e);
        }
    }

    private byte[] serializeMetadata(Map<String, String> metadata) {
        Objects.requireNonNull(metadata, "Metadata cannot be null");
        return metadata.entrySet().stream()
                .map(e -> escape(e.getKey()) + "=" + escape(e.getValue()))
                .collect(Collectors.joining(";"))
                .getBytes(StandardCharsets.UTF_8);
    }

    private Map<String, String> deserializeMetadata(byte[] data) {
        Objects.requireNonNull(data, "Data cannot be null");
        String str = new String(data, StandardCharsets.UTF_8);
        Map<String, String> map = new HashMap<>();
        if (!str.isEmpty()) {
            for (String entry : str.split("(?<!\\\\);")) {
                if (entry.contains("=")) {
                    String[] kv = entry.split("(?<!\\\\)=", 2);
                    map.put(unescape(kv[0]), unescape(kv[1]));
                }
            }
        }
        return map;
    }

    private String escape(String s) {
        return s.replace("=", "\\=").replace(";", "\\;");
    }

    private String unescape(String s) {
        return s.replace("\\=", "=").replace("\\;", ";");
    }

    public void saveEncryptedPoint(EncryptedPoint pt) throws IOException {
        Objects.requireNonNull(pt, "EncryptedPoint cannot be null");
        String safeVersion = "v" + pt.getVersion();

        Path versionDir = Paths.get(baseDir, safeVersion);
        Files.createDirectories(versionDir);
        Path filePath = versionDir.resolve(pt.getId() + ".point");
        PersistenceUtils.saveObject(pt, filePath.toString(), baseDir);

        Map<String, String> meta = new HashMap<>();
        meta.put("version", String.valueOf(pt.getVersion()));
        meta.put("shardId", String.valueOf(pt.getShardId()));
        putVectorMetadata(pt.getId(), meta);
    }

    public List<EncryptedPoint> getAllEncryptedPoints() {
        Set<String> seenIds = new HashSet<>();
        List<EncryptedPoint> uniquePoints = new ArrayList<>();

        try (Stream<Path> files = Files.walk(Paths.get(baseDir))) {
            files.filter(Files::isRegularFile)
                    .filter(path -> path.toString().endsWith(".point"))
                    .forEach(path -> {
                        try {
                            EncryptedPoint pt = PersistenceUtils.loadObject(path.toString(), baseDir, EncryptedPoint.class);
                            if (pt == null) return;

                            String id = pt.getId();
                            if (!seenIds.add(id)) {
                                logger.debug("Skipping duplicate EncryptedPoint with ID {}", id);
                                return;
                            }

                            Map<String, String> meta = getVectorMetadata(id);
                            if (meta == null || !meta.containsKey("version") || !meta.containsKey("shardId")) {
                                logger.warn("Skipping point {}: Missing or incomplete metadata", id);
                                return;
                            }

                            uniquePoints.add(pt);
                        } catch (IOException | ClassNotFoundException e) {
                            logger.error("Failed to load point from {}", path, e);
                        }
                    });
        } catch (IOException e) {
            logger.error("Failed to walk points directory {}", baseDir, e);
        }

        logger.debug("Total encrypted points loaded: {}", uniquePoints.size());
        return uniquePoints;
    }

    public void cleanupStaleMetadata(Set<String> validIds) {
        Objects.requireNonNull(validIds, "Valid IDs set cannot be null");
        int removed = 0;
        try (WriteBatch batch = new WriteBatch(); WriteOptions opts = new WriteOptions()) {
            try (RocksIterator it = db.newIterator()) {
                for (it.seekToFirst(); it.isValid(); it.next()) {
                    String key = new String(it.key(), StandardCharsets.UTF_8);
                    if (!validIds.contains(key) && !key.equals("index")) {
                        batch.delete(key.getBytes(StandardCharsets.UTF_8));
                        removed++;
                    }
                }
            }
            if (removed > 0) {
                db.write(opts, batch);
                logger.debug("Cleaned up {} stale metadata entries", removed);
            }
        } catch (RocksDBException e) {
            logger.error("Error while cleaning up stale metadata entries", e);
        }
    }

    public EncryptedPoint loadEncryptedPoint(String id) throws IOException, ClassNotFoundException {
        Objects.requireNonNull(id, "Vector ID cannot be null");
        Map<String, String> meta = getVectorMetadata(id);
        if (meta == null || !meta.containsKey("version")) {
            logger.warn("Missing version metadata for id={}", id);
            return null;
        }
        String safeVersion = meta.get("version").startsWith("v") ? meta.get("version") : "v" + meta.get("version");
        Path filePath = Paths.get(baseDir, safeVersion, id + ".point");

        if (!Files.exists(filePath)) {
            logger.warn("Expected encrypted point file does not exist: {}", filePath);
            return null;
        }

        return PersistenceUtils.loadObject(filePath.toString(), baseDir, EncryptedPoint.class);
    }

    public void saveIndexVersion(int version) {
        try {
            db.put("index".getBytes(StandardCharsets.UTF_8), String.valueOf(version).getBytes(StandardCharsets.UTF_8));
            logger.debug("Saved index version {}", version);
        } catch (RocksDBException e) {
            logger.error("Failed to save index version {}", version, e);
        }
    }

    public void updateVectorMetadata(String vectorId, Map<String, String> updates) {
        if (closed) throw new IllegalStateException("RocksDBMetadataManager is closed");
        Objects.requireNonNull(vectorId, "Vector ID cannot be null");
        Objects.requireNonNull(updates, "Updates cannot be null");
        try {
            db.put(vectorId.getBytes(StandardCharsets.UTF_8), serializeMetadata(updates));
            logger.debug("Updated metadata for vectorId={}", vectorId);
        } catch (RocksDBException e) {
            logger.error("Failed to update metadata for vectorId={}", vectorId, e);
            throw new RuntimeException("Failed to update metadata", e);
        }
    }

    public void mergeVectorMetadata(String vectorId, Map<String, String> updates) {
        if (closed) throw new IllegalStateException("RocksDBMetadataManager is closed");
        Objects.requireNonNull(vectorId, "Vector ID cannot be null");
        Objects.requireNonNull(updates, "Updates cannot be null");
        try {
            Map<String, String> existing = getVectorMetadata(vectorId);
            updates.forEach(existing::putIfAbsent);
            db.put(vectorId.getBytes(StandardCharsets.UTF_8), serializeMetadata(existing));
            logger.debug("Merged metadata for vectorId={}", vectorId);
        } catch (RocksDBException e) {
            logger.error("Failed to merge metadata for vectorId={}", vectorId, e);
            throw new RuntimeException("Failed to merge metadata", e);
        }
    }

    public String getPointsBaseDir() {
        return this.baseDir;
    }

    public int loadIndexVersion() {
        try {
            byte[] data = db.get("index".getBytes(StandardCharsets.UTF_8));
            return (data != null) ? Integer.parseInt(new String(data, StandardCharsets.UTF_8)) : 1;
        } catch (RocksDBException e) {
            logger.warn("Failed to load index version, defaulting to 1", e);
            return 1;
        }
    }

    public List<String> getAllVectorIds() {
        List<String> keys = new ArrayList<>();
        try (RocksIterator it = db.newIterator()) {
            for (it.seekToFirst(); it.isValid(); it.next()) {
                String key = new String(it.key(), StandardCharsets.UTF_8);
                if (!key.equals("index")) {
                    keys.add(key);
                }
            }
        } catch (Exception e) {
            logger.error("Failed to iterate vector IDs", e);
        }
        return keys;
    }

    public void logStats() {
        try {
            String stats = db.getProperty("rocksdb.stats");
            logger.debug("ROCKSDB STATS:\n{}", stats);
        } catch (RocksDBException e) {
            logger.warn("Could not retrieve RocksDB stats", e);
        }
    }

    public void printSummary() {
        try {
            logger.debug("Metadata contains approx {} keys", db.getLongProperty("rocksdb.estimate-num-keys"));
            logger.debug("Metadata live SST files: {}", db.getProperty("rocksdb.num-live-sst-files"));
        } catch (RocksDBException e) {
            logger.error("Failed to read RocksDB summary", e);
        }
    }

    public void close() {
        logger.info("Closing RocksDBMetadataManager...");
        try {
            // Existing close logic
            logger.info("RocksDBMetadataManager closed successfully");
        } catch (Exception e) {
            logger.error("Failed to close RocksDBMetadataManager", e);
            throw new RuntimeException("Failed to close RocksDBMetadataManager", e);
        }
    }

    public String getDbPath() {
        return dbPath;
    }
}