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

    public void putVectorMetadata(String vectorId, Map<String, String> metadata) {
        try {
            Objects.requireNonNull(vectorId);
            Objects.requireNonNull(metadata);
            Map<String, String> existing = getVectorMetadata(vectorId);
            existing.putAll(metadata);
            db.put(vectorId.getBytes(StandardCharsets.UTF_8), serializeMetadata(existing));
            logger.debug("🟡 db.put({}): {}", vectorId, existing);
        } catch (Exception e) {
            logger.error("Failed to put metadata for vectorId={}", vectorId, e);
        }
    }

    public Map<String, String> getVectorMetadata(String vectorId) {
        if (closed) throw new IllegalStateException("RocksDBMetadataManager is closed");
        try {
            byte[] value = db.get(vectorId.getBytes(StandardCharsets.UTF_8));
            return (value != null) ? deserializeMetadata(value) : new HashMap<>();
        } catch (Exception e) {
            logger.warn("Failed to get metadata for vectorId={}", vectorId, e);
            return new HashMap<>();
        }
    }

    public void removeVectorMetadata(String vectorId) {
        try {
            db.delete(vectorId.getBytes(StandardCharsets.UTF_8));
            logger.info("🧹 Deleted metadata for vectorId={}", vectorId);
        } catch (Exception e) {
            logger.warn("Failed to delete metadata for vectorId={}", vectorId, e);
        }
    }

    private byte[] serializeMetadata(Map<String, String> metadata) {
        return metadata.entrySet().stream()
                .map(e -> escape(e.getKey()) + "=" + escape(e.getValue()))
                .collect(Collectors.joining(";"))
                .getBytes(StandardCharsets.UTF_8);
    }

    private Map<String, String> deserializeMetadata(byte[] data) {
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
        Objects.requireNonNull(pt);
        String versionStr = getVectorMetadata(pt.getId()).getOrDefault("version", "v1");
        String safeVersion = versionStr.startsWith("v") ? versionStr : "v" + versionStr;
        Path versionDir = Paths.get(baseDir, safeVersion);
        Files.createDirectories(versionDir);
        Path filePath = versionDir.resolve(pt.getId() + ".point");
        PersistenceUtils.saveObject(pt, filePath.toString());
    }

    @SuppressWarnings("unchecked")
    public List<EncryptedPoint> getAllEncryptedPoints() {
        Set<String> seenIds = new HashSet<>();
        List<EncryptedPoint> uniquePoints = new ArrayList<>();

        try (Stream<Path> files = Files.walk(Paths.get(baseDir))) {
            files.filter(Files::isRegularFile)
                    .filter(path -> path.toString().endsWith(".point"))
                    .forEach(path -> {
                        try {
                            EncryptedPoint pt = PersistenceUtils.loadObject(path.toString());
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

        logger.info("✅ Total encrypted points loaded (unique + metadata-valid): {}", uniquePoints.size());
        return uniquePoints;
    }

    public void cleanupStaleMetadata(Set<String> validIds) {
        int removed = 0;
        try (RocksIterator it = db.newIterator()) {
            for (it.seekToFirst(); it.isValid(); it.next()) {
                String key = new String(it.key(), StandardCharsets.UTF_8);
                if (!validIds.contains(key)) {
                    db.delete(key.getBytes(StandardCharsets.UTF_8));
                    removed++;
                }
            }
        } catch (Exception e) {
            logger.error("Error while cleaning up stale metadata entries", e);
        }
        logger.info("🧹 Cleaned up {} stale metadata entries.", removed);
    }

    @SuppressWarnings("unchecked")
    public EncryptedPoint loadEncryptedPoint(String id) throws IOException, ClassNotFoundException {
        try (Stream<Path> paths = Files.walk(Paths.get(baseDir), 3)) {
            return paths
                    .filter(p -> p.getFileName().toString().equals(id + ".point"))
                    .map(p -> {
                        try {
                            return (EncryptedPoint) PersistenceUtils.loadObject(p.toString());
                        } catch (Exception e) {
                            logger.error("Failed to load encrypted point: {}", p, e);
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .findFirst()
                    .orElse(null);
        }
    }

    public void saveIndexVersion(int version) {
        try {
            db.put("index".getBytes(StandardCharsets.UTF_8), String.valueOf(version).getBytes(StandardCharsets.UTF_8));
        } catch (RocksDBException e) {
        }
    }

    public void updateVectorMetadata(String vectorId, Map<String, String> updates) {
        if (closed) {
            throw new IllegalStateException("MetadataManager is closed");
        }
        try {
            Objects.requireNonNull(vectorId, "Vector ID must not be null");
            Objects.requireNonNull(updates, "Updates must not be null");
            Map<String, String> existing = getVectorMetadata(vectorId);
            if (existing.isEmpty()) {
            }
            existing.putAll(updates);
            putVectorMetadata(vectorId, existing);
        } catch (Exception e) {
        }
    }

    public void mergeVectorMetadata(String vectorId, Map<String, String> updates) {
        if (closed) {
            throw new IllegalStateException("MetadataManager is closed");
        }
        try {
            Objects.requireNonNull(vectorId, "Vector ID must not be null");
            Objects.requireNonNull(updates, "Updates must not be null");

            Map<String, String> existing = getVectorMetadata(vectorId);
            updates.forEach(existing::putIfAbsent);
            db.put(vectorId.getBytes(StandardCharsets.UTF_8), serializeMetadata(existing));

            if (logger.isDebugEnabled()) {
                logger.debug("Merged metadata for vectorId={}: {}", vectorId, existing);
            }
        } catch (Exception e) {
            logger.error("Failed to merge metadata for vectorId={}", vectorId, e);
        }
    }

    public String getPointsBaseDir() {
        return this.baseDir;
    }

    public int loadIndexVersion() {
        try {
            byte[] data = db.get("index".getBytes(StandardCharsets.UTF_8));
            return (data != null) ? Integer.parseInt(new String(data, StandardCharsets.UTF_8)) : 1;
        } catch (Exception e) {
            logger.warn("Failed to load index version, defaulting to 1", e);
            return 1;
        }
    }

    public List<String> getAllVectorIds() {
        List<String> keys = new ArrayList<>();
        try (RocksIterator it = db.newIterator()) {
            for (it.seekToFirst(); it.isValid(); it.next()) {
                keys.add(new String(it.key(), StandardCharsets.UTF_8));
            }
        } catch (Exception e) {
            logger.error("Failed to iterate vector IDs", e);
        }
        return keys;
    }

    @Override
    public void close() {
        if (!closed) {
            logger.info("Closing RocksDB instance...");
            db.close();
            closed = true;
            logger.info("RocksDB instance closed at {}", dbPath);
        }
    }

    public String getDbPath() {
        return dbPath;
    }

}
