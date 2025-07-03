package com.fspann.common;

import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
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
            logger.info("RocksDB native library loaded successfully");
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
            options.close(); // ✅ prevents native memory leaks
        }
    }

    public void putVectorMetadata(String vectorId, Map<String, String> metadata) {
        try {
            Objects.requireNonNull(vectorId);
            Objects.requireNonNull(metadata);
            Map<String, String> existing = getVectorMetadata(vectorId);
            existing.putAll(metadata);
            db.put(vectorId.getBytes(StandardCharsets.UTF_8), serializeMetadata(existing));
        } catch (Exception e) {
            logger.error("Failed to put metadata for vectorId={}", vectorId, e);
        }
    }

    public Map<String, String> getVectorMetadata(String vectorId) {
        try {
            Objects.requireNonNull(vectorId);
            byte[] value = db.get(vectorId.getBytes(StandardCharsets.UTF_8));
            return value != null ? deserializeMetadata(value) : new HashMap<>();
        } catch (Exception e) {
            logger.warn("Failed to get metadata for vectorId={}", vectorId, e);
            return new HashMap<>();
        }
    }

    private byte[] serializeMetadata(Map<String, String> metadata) {
        StringBuilder sb = new StringBuilder();
        metadata.forEach((k, v) -> sb.append(escape(k)).append('=').append(escape(v)).append(';'));
        return sb.toString().getBytes(StandardCharsets.UTF_8);
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
        return s == null ? "" : s.replace("=", "\\=").replace(";", "\\;");
    }

    private String unescape(String s) {
        return s.replace("\\=", "=").replace("\\;", ";");
    }

    public void updateVectorMetadata(String vectorId, Map<String, String> updates) {
        try {
            Objects.requireNonNull(vectorId);
            Objects.requireNonNull(updates);
            Map<String, String> existing = getVectorMetadata(vectorId);
            existing.putAll(updates);
            putVectorMetadata(vectorId, existing);
        } catch (Exception e) {
            logger.error("Failed to update metadata for vectorId={}", vectorId, e);
        }
    }

    public Set<String> getAllVectorIds() {
        Set<String> keys = new HashSet<>();
        try (RocksIterator it = db.newIterator()) {
            for (it.seekToFirst(); it.isValid(); it.next()) {
                keys.add(new String(it.key(), StandardCharsets.UTF_8));
            }
        } catch (Exception e) {
            logger.error("Failed to iterate vector IDs", e);
        }
        return keys;
    }

    public void removeVectorMetadata(String vectorId) {
        try {
            Objects.requireNonNull(vectorId);
            db.delete(vectorId.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            logger.warn("Failed to delete metadata for vectorId={}", vectorId, e);
        }
    }

    @Override
    public void close() {
        if (closed) return;
        try {
            if (db != null) {
                db.flush(new FlushOptions());
                db.close();
                logger.info("RocksDB closed at {}", dbPath);
            }
        } catch (Exception e) {
            logger.error("Failed to close RocksDB", e);
        } finally {
            closed = true;
        }
    }


    public void saveEncryptedPoint(EncryptedPoint pt) throws IOException {
        Objects.requireNonNull(pt);
        String versionStr = getVectorMetadata(pt.getId()).getOrDefault("version", "v1");
        if (!versionStr.matches("[a-zA-Z0-9_]+")) {
            throw new IllegalArgumentException("Invalid version format: " + versionStr);
        }

        Path versionDir = Paths.get(baseDir).resolve("v" + versionStr);
        Files.createDirectories(versionDir);

        Path filePath = versionDir.resolve(pt.getId() + ".point");
        PersistenceUtils.saveObject(pt, filePath.toString());
        logger.info("Saved encrypted point: {} at {}", pt.getId(), filePath);
    }

    public List<EncryptedPoint> getAllEncryptedPoints() {
        List<EncryptedPoint> points = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(Paths.get(baseDir))) {
            paths.filter(Files::isRegularFile).forEach(path -> {
                try {
                    if (path.toString().endsWith(".points")) {
                        List<EncryptedPoint> batchPoints = PersistenceUtils.loadObject(path.toString());
                        if (batchPoints != null) points.addAll(batchPoints);
                    } else if (path.toString().endsWith(".point")) {
                        EncryptedPoint pt = PersistenceUtils.loadObject(path.toString());
                        if (pt != null) points.add(pt);
                    }
                } catch (Exception e) {
                    logger.error("Failed to load encrypted point from: {}", path, e);
                }
            });
        } catch (IOException e) {
            logger.error("Failed to walk through points directory", e);
        }
        return points;
    }

    public EncryptedPoint loadEncryptedPoint(String id) throws IOException, ClassNotFoundException {
        Objects.requireNonNull(id, "EncryptedPoint ID must not be null");

        try (Stream<Path> paths = Files.walk(Paths.get(baseDir))) {
            return paths
                    .filter(p -> p.getFileName().toString().equals(id + ".point"))
                    .map(p -> {
                        try {
                            return PersistenceUtils.<EncryptedPoint>loadObject(p.toString());
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

    public Stream<String> streamAllVectorIds() {
        RocksIterator it = db.newIterator();
        it.seekToFirst();
        return Stream.generate(() -> {
            if (it.isValid()) {
                String key = new String(it.key(), StandardCharsets.UTF_8);
                it.next();
                return key;
            }
            return null;
        }).takeWhile(Objects::nonNull).onClose(it::close);
    }

    public Stream<EncryptedPoint> streamAllEncryptedPoints() {
        try {
            return Files.walk(Paths.get(baseDir))
                    .filter(Files::isRegularFile)
                    .flatMap(path -> {
                        try {
                            if (path.toString().endsWith(".points")) {
                                List<EncryptedPoint> list = PersistenceUtils.loadObject(path.toString());
                                return list != null ? list.stream() : Stream.empty();
                            } else if (path.toString().endsWith(".point")) {
                                EncryptedPoint pt = PersistenceUtils.loadObject(path.toString());
                                return pt != null ? Stream.of(pt) : Stream.empty();
                            }
                        } catch (Exception e) {
                            logger.error("Failed to load encrypted point from: {}", path, e);
                        }
                        return Stream.empty();
                    });
        } catch (IOException e) {
            logger.error("Failed to walk points directory: {}", baseDir, e);
            return Stream.empty();
        }
    }

    public String getDbPath() {
        return dbPath;
    }

}
