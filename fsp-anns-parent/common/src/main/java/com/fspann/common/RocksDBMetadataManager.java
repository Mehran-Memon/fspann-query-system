package com.fspann.common;

import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RocksDBMetadataManager implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(RocksDBMetadataManager.class);
    private static final Object LOCK = new Object();
    private static RocksDBMetadataManager instance = null;
    private volatile boolean syncWrites = false;
    private final StorageMetrics storageMetrics;

    private final RocksDB db;
    private final String dbPath;
    private final String baseDir;
    private final Options options;
    private volatile boolean closed = false;

    private static final Object INIT_LOCK = new Object();

    // Track old point files for deferred cleanup (ISSUE #3 FIX)
    private final Queue<Path> oldPointFilesForCleanup = new ConcurrentLinkedQueue<>();

    static {
        try {
            RocksDB.loadLibrary();
            logger.info("RocksDB native library loaded.");
        } catch (Throwable t) {
            throw new RuntimeException("Failed to load RocksDB native library", t);
        }
    }

    private static RocksDBMetadataManager createDefaultMetadataManager() {
        try {
            return RocksDBMetadataManager.create(
                    FsPaths.metadataDb().toString(),
                    FsPaths.pointsDir().toString()
            );
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize RocksDBMetadataManager", e);
        }
    }

    public static RocksDBMetadataManager create(String dbPath, String pointsPath) throws IOException {
        // Check without lock first (optimization)
        if (instance != null && !instance.closed) {
            return instance;
        }

        // Initialize without holding global lock
        RocksDBMetadataManager newInstance = new RocksDBMetadataManager(dbPath, pointsPath);

        // Only acquire lock for assignment
        synchronized (LOCK) {
            if (instance != null && !instance.closed) {
                // Someone else created it while we were initializing
                newInstance.close();
                return instance;
            }
            instance = newInstance;
            return instance;
        }
    }

    private RocksDBMetadataManager(String dbPath, String baseDir) throws IOException {
        this.dbPath = dbPath;
        this.baseDir = baseDir;

        Files.createDirectories(Paths.get(dbPath));
        Files.createDirectories(Paths.get(baseDir));

        this.options = new Options()
                .setCreateIfMissing(true)
                .setWriteBufferSize(16 * 1024 * 1024)
                .setMaxBackgroundJobs(2)
                .setCompressionType(CompressionType.NO_COMPRESSION)
                .setInfoLogLevel(InfoLogLevel.ERROR_LEVEL);

        try {
            this.db = RocksDB.open(this.options, dbPath);
        } catch (RocksDBException e) {
            throw new IOException("RocksDB open failed at " + dbPath, e);
        }
        logger.debug("RocksDBMetadataManager opened at {} (points at {})", dbPath, baseDir);

        Path pointsDir = Paths.get(baseDir);
        Path metaDir = Paths.get(dbPath);
        this.storageMetrics = new StorageMetrics(pointsDir, metaDir);
        logger.info("StorageMetrics initialized for base={}, db={}", baseDir, dbPath);

        this.closed = false;
    }

    @Override
    public void close() {
        synchronized (LOCK) {
            if (closed) return;
            try {
                if (db != null) {
                    try { db.syncWal(); } catch (Exception ignore) {}
                    db.close();
                }
            } catch (Throwable t) {
                logger.warn("Error closing RocksDB at {}", dbPath, t);
            } finally {
                try { options.close(); } catch (Throwable ignore) {}
                if (storageMetrics != null) {
                    logger.info("Final storage snapshot: {}", storageMetrics.getSummary());
                }
                closed = true;
                instance = null;
                logger.debug("RocksDBMetadataManager closed for {}", dbPath);
            }
        }
    }

    // ------------------- CRUD: metadata -------------------

    public synchronized Map<String, Map<String, String>> multiGetVectorMetadata(Collection<String> vectorIds) {
        Objects.requireNonNull(vectorIds, "vectorIds");
        if (vectorIds.isEmpty()) return Collections.emptyMap();

        Map<String, Map<String, String>> out = new LinkedHashMap<>(vectorIds.size());
        for (String id : vectorIds) {
            out.put(id, getVectorMetadata(id));
        }
        return out;
    }

    public synchronized Map<String, String> getVectorMetadata(String vectorId) {
        Objects.requireNonNull(vectorId, "vectorId");
        try {
            byte[] v = db.get(vectorId.getBytes(StandardCharsets.UTF_8));
            return (v == null) ? Collections.emptyMap() : deserializeMetadata(v);
        } catch (RocksDBException e) {
            logger.warn("getVectorMetadata failed for {}", vectorId, e);
            return Collections.emptyMap();
        }
    }

    public synchronized void updateVectorMetadata(String vectorId, Map<String, String> updates) {
        Objects.requireNonNull(vectorId, "vectorId");
        Objects.requireNonNull(updates, "updates");
        try {
            db.put(vectorId.getBytes(StandardCharsets.UTF_8), serializeMetadata(updates));
        } catch (RocksDBException e) {
            throw new RuntimeException("updateVectorMetadata failed for " + vectorId, e);
        }
    }

    public synchronized void mergeVectorMetadata(String vectorId, Map<String, String> updates) {
        Objects.requireNonNull(vectorId, "vectorId");
        Objects.requireNonNull(updates, "updates");
        Map<String, String> existing = getVectorMetadata(vectorId);
        if (existing.isEmpty()) {
            existing = new HashMap<>(updates);
        } else {
            updates.forEach(existing::putIfAbsent);
        }
        updateVectorMetadata(vectorId, existing);
    }

    public synchronized void removeVectorMetadata(String vectorId) {
        Objects.requireNonNull(vectorId, "vectorId");
        try {
            db.delete(vectorId.getBytes(StandardCharsets.UTF_8));
        } catch (RocksDBException e) {
            logger.warn("removeVectorMetadata failed for {}", vectorId, e);
        }
    }

    /**
     * Check if a vector is marked as deleted.
     *
     * @param vectorId the vector ID
     * @return true if deleted, false otherwise (or if not found)
     */
    public boolean isDeleted(String vectorId) {
        Objects.requireNonNull(vectorId, "vectorId cannot be null");

        try {
            Map<String, String> meta = getVectorMetadata(vectorId);
            if (meta == null) {
                return false;
            }

            String deletedFlag = meta.getOrDefault("deleted", "false");
            boolean isDeleted = "true".equalsIgnoreCase(deletedFlag);

            if (isDeleted) {
                logger.trace("Vector {} is marked deleted", vectorId);
            }

            return isDeleted;
        } catch (Exception e) {
            logger.warn("Error checking deletion status of {}: {}", vectorId, e.getMessage());
            return false;
        }
    }

    /**
     * Get the timestamp when a vector was deleted.
     *
     * @param vectorId the vector ID
     * @return deletion timestamp in milliseconds, or -1 if not deleted or not found
     */
    public long getDeletedTimestamp(String vectorId) {
        Objects.requireNonNull(vectorId, "vectorId cannot be null");

        try {
            Map<String, String> meta = getVectorMetadata(vectorId);
            if (meta == null) {
                return -1L;
            }

            String deletedFlag = meta.getOrDefault("deleted", "false");
            if (!"true".equalsIgnoreCase(deletedFlag)) {
                return -1L;
            }

            String timestampStr = meta.get("deleted_at");
            if (timestampStr == null) {
                logger.warn("Vector {} marked deleted but no timestamp", vectorId);
                return -1L;
            }

            long timestamp = Long.parseLong(timestampStr);
            logger.trace("Vector {} deleted at timestamp {}", vectorId, timestamp);
            return timestamp;
        } catch (NumberFormatException e) {
            logger.warn("Invalid deletion timestamp for {}: {}", vectorId, e.getMessage());
            return -1L;
        } catch (Exception e) {
            logger.warn("Error getting deletion timestamp for {}: {}", vectorId, e.getMessage());
            return -1L;
        }
    }

    /**
     * Get count of deleted vectors (for metrics).
     * Note: This is expensive as it scans all metadata.
     *
     * @return count of vectors marked as deleted
     */
    public int countDeletedVectors() {
        try {
            int count = 0;
            List<String> allIds = getAllVectorIds();
            for (String id : allIds) {
                if (isDeleted(id)) {
                    count++;
                }
            }
            logger.info("Total deleted vectors: {}", count);
            return count;
        } catch (Exception e) {
            logger.warn("Error counting deleted vectors", e);
            return -1;
        }
    }

    /**
     * Permanently remove a deleted vector from metadata (hard delete).
     * Only call this after re-encryption if you want to reclaim space.
     *
     * ISSUE #1 FIX: Now properly synchronized to prevent TOCTOU race condition
     *
     * @param vectorId the vector ID
     */
    public synchronized void hardDeleteVector(String vectorId) {
        Objects.requireNonNull(vectorId, "vectorId cannot be null");

        if (!isDeleted(vectorId)) {
            logger.warn("Attempted hard delete of non-deleted vector {}", vectorId);
            return;
        }

        try {
            removeVectorMetadata(vectorId);
            logger.info("Hard deleted vector {} (removed from metadata)", vectorId);
        } catch (Exception e) {
            logger.error("Failed to hard delete vector {}", vectorId, e);
        }
    }

    public synchronized void batchUpdateVectorMetadata(Map<String, Map<String, String>> updates) throws IOException {
        Objects.requireNonNull(updates, "updates");
        try (WriteBatch batch = new WriteBatch(); WriteOptions wo = new WriteOptions()) {
            for (Map.Entry<String, Map<String, String>> e : updates.entrySet()) {
                batch.put(e.getKey().getBytes(StandardCharsets.UTF_8), serializeMetadata(e.getValue()));
            }
            db.write(wo, batch);
        } catch (RocksDBException e) {
            throw new IOException("batchUpdateVectorMetadata failed", e);
        }
    }

    public synchronized void batchPutMetadata(Map<String, Map<String, String>> allMetadata) {
        Objects.requireNonNull(allMetadata, "allMetadata");
        try (WriteBatch batch = new WriteBatch(); WriteOptions wo = new WriteOptions().setSync(syncWrites)) {
            for (Map.Entry<String, Map<String, String>> e : allMetadata.entrySet()) {
                batch.put(e.getKey().getBytes(StandardCharsets.UTF_8), serializeMetadata(e.getValue()));
            }
            db.write(wo, batch);
        } catch (RocksDBException e) {
            throw new RuntimeException("batchPutMetadata failed", e);
        }
    }

    // ------------------- points persistence -------------------

    /**
     * ISSUE #2 FIX: Now properly synchronized to prevent concurrent writes
     * ISSUE #3 FIX: Deferred secure deletion (moved to background cleanup)
     * ISSUE #4 FIX: Metadata written FIRST (atomic in RocksDB), then point file
     */
    public synchronized void saveEncryptedPoint(EncryptedPoint pt) throws IOException {
        Objects.requireNonNull(pt, "pt");

        String safeVersion = "v" + pt.getVersion();
        Path versionDir = Paths.get(baseDir, safeVersion);
        Files.createDirectories(versionDir);

        // ISSUE #4 FIX: Step 1 - Update metadata FIRST (RocksDB is atomic)
        Map<String, String> meta = new HashMap<>();
        meta.put("version", String.valueOf(pt.getVersion()));
        meta.put("shardId", String.valueOf(pt.getShardId()));
        meta.put("dim", String.valueOf(pt.getVectorLength()));
        try {
            updateVectorMetadata(pt.getId(), meta);
        } catch (RuntimeException e) {
            logger.error("Failed to update metadata for {}", pt.getId(), e);
            throw e;
        }

        // ISSUE #4 FIX: Step 2 - NOW save point file
        Path tmp = versionDir.resolve(pt.getId() + ".point.tmp");
        Path dst = versionDir.resolve(pt.getId() + ".point");

        try {
            PersistenceUtils.saveObject(pt, tmp.toString(), baseDir);
            Files.move(tmp, dst, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            logger.error("Failed to save point file for {}", pt.getId(), e);
            throw e;
        }

        // ISSUE #3 FIX: Queue old version files for cleanup, don't do it now
        queueOldVersionFileForCleanup(pt.getId(), pt.getVersion());
    }

    /**
     * ISSUE #2 FIX: Now properly synchronized
     * ISSUE #3 FIX: Batch operations don't do per-point secure deletion
     */
    public synchronized void saveEncryptedPointsBatch(Collection<EncryptedPoint> points) throws IOException {
        if (points == null || points.isEmpty()) return;

        // Step 1: Save all point files (no secure deletion per-point)
        List<Path> tmps = new ArrayList<>(points.size());
        List<Path> dsts = new ArrayList<>(points.size());
        for (EncryptedPoint pt : points) {
            String safeVersion = "v" + pt.getVersion();
            Path versionDir = Paths.get(baseDir, safeVersion);
            Files.createDirectories(versionDir);
            Path tmp = versionDir.resolve(pt.getId() + ".point.tmp");
            Path dst = versionDir.resolve(pt.getId() + ".point");
            PersistenceUtils.saveObject(pt, tmp.toString(), baseDir);
            tmps.add(tmp);
            dsts.add(dst);
        }

        // Step 2: Move all files atomically
        for (int i = 0; i < tmps.size(); i++) {
            Files.move(tmps.get(i), dsts.get(i), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        }

        // Step 3: Batch update metadata
        Map<String, Map<String, String>> allMeta = new LinkedHashMap<>();
        for (EncryptedPoint pt : points) {
            Map<String, String> m = new HashMap<>();
            m.put("version", String.valueOf(pt.getVersion()));
            m.put("shardId", String.valueOf(pt.getShardId()));
            m.put("dim", String.valueOf(pt.getVectorLength()));
            List<Integer> buckets = pt.getBuckets();
            if (buckets != null) {
                for (int t = 0; t < buckets.size(); t++) {
                    m.put("b" + t, String.valueOf(buckets.get(t)));
                }
            }
            allMeta.put(pt.getId(), m);
        }
        batchPutMetadata(allMeta);

        // ISSUE #3 FIX: Queue old version files for cleanup
        for (EncryptedPoint pt : points) {
            queueOldVersionFileForCleanup(pt.getId(), pt.getVersion());
        }
    }

    /**
     * ISSUE #3 FIX: Queue old version files for deferred cleanup
     * Call cleanupOldVersionFiles() during low-activity periods
     */
    private void queueOldVersionFileForCleanup(String pointId, int currentVersion) {
        try (DirectoryStream<Path> dirs = Files.newDirectoryStream(Paths.get(baseDir))) {
            for (Path dir : dirs) {
                if (!Files.isDirectory(dir)) continue;
                String name = dir.getFileName().toString();

                if (!name.startsWith("v")) continue;

                int ver;
                try {
                    ver = Integer.parseInt(name.substring(1));
                } catch (Exception ignore) {
                    continue;
                }

                if (ver >= currentVersion) continue;

                Path old = dir.resolve(pointId + ".point");
                if (Files.exists(old)) {
                    oldPointFilesForCleanup.offer(old);
                }
            }
        } catch (IOException ignore) {
            // Best effort
        }
    }

    /**
     * ISSUE #3 FIX: Background cleanup for old version files
     * Call this during low-activity periods (e.g., off-peak hours)
     *
     * @param maxOldFilesToProcess maximum number of files to process in one call
     * @return number of files actually cleaned up
     */
    public int cleanupOldVersionFilesDeferred(int maxOldFilesToProcess) {
        int cleaned = 0;
        int processed = 0;

        while (processed < maxOldFilesToProcess && !oldPointFilesForCleanup.isEmpty()) {
            Path old = oldPointFilesForCleanup.poll();
            if (old == null) break;

            try {
                if (Files.exists(old)) {
                    // Secure deletion: zero out file before deleting
                    try {
                        byte[] zeros = new byte[(int) Files.size(old)];
                        Files.write(old, zeros);
                    } catch (Throwable ignore) {
                        // If overwrite fails, proceed to delete
                    }

                    Files.deleteIfExists(old);
                    cleaned++;
                    logger.debug("Cleaned up old point file: {}", old);
                }
            } catch (Exception e) {
                logger.warn("Failed to cleanup old point file {}", old, e);
            }
            processed++;
        }

        if (cleaned > 0) {
            logger.info("Cleaned up {} old version files ({} remaining in queue)",
                    cleaned, oldPointFilesForCleanup.size());
        }

        return cleaned;
    }

    /**
     * ISSUE #6 FIX: Improved getAllEncryptedPoints()
     * Now uses metadata as source of truth, loads only referenced points
     */
    public List<EncryptedPoint> getAllEncryptedPoints() {
        List<EncryptedPoint> list = new ArrayList<>();
        try {
            // Enumerate from metadata (source of truth)
            List<String> allIds = getAllVectorIds();

            for (String id : allIds) {
                try {
                    EncryptedPoint pt = loadEncryptedPoint(id);
                    if (pt != null) {
                        list.add(pt);
                    }
                } catch (IOException | ClassNotFoundException e) {
                    logger.warn("Failed to load point {}", id, e);
                }
            }

            logger.info("Loaded {} encrypted points from disk ({} unique ids).",
                    list.size(),
                    list.stream().map(EncryptedPoint::getId).distinct().count());
        } catch (Exception e) {
            logger.error("getAllEncryptedPoints failed", e);
        }
        return list;
    }

    public EncryptedPoint loadEncryptedPoint(String id) throws IOException, ClassNotFoundException {
        Objects.requireNonNull(id, "id");
        Map<String, String> meta = getVectorMetadata(id);
        if (meta.isEmpty() || !meta.containsKey("version")) {
            return null;
        }

        String ver = meta.get("version");
        String safeVersion = ver.startsWith("v") ? ver : "v" + ver;
        Path p = Paths.get(baseDir, safeVersion, id + ".point");
        if (!Files.exists(p)) {
            return null;
        }
        return PersistenceUtils.loadObject(p.toString(), baseDir, EncryptedPoint.class);
    }

    public void cleanupStaleMetadata(Set<String> validIds) {
        Objects.requireNonNull(validIds, "validIds");
        RocksIterator it = null;
        try {
            it = db.newIterator();
            List<byte[]> toDelete = new ArrayList<>();
            for (it.seekToFirst(); it.isValid(); it.next()) {
                String key = new String(it.key(), StandardCharsets.UTF_8);
                if (!"index".equals(key) && !validIds.contains(key)) {
                    toDelete.add(it.key());
                }
            }
            if (!toDelete.isEmpty()) {
                try (WriteBatch batch = new WriteBatch(); WriteOptions wo = new WriteOptions()) {
                    for (byte[] k : toDelete) {
                        batch.delete(k);
                    }
                    db.write(wo, batch);
                }
            }
        } catch (RocksDBException e) {
            logger.warn("cleanupStaleMetadata failed", e);
        } finally {
            if (it != null) {
                it.close();
            }
        }
    }

    // ------------------- index version helpers -------------------

    public void saveIndexVersion(int version) {
        try {
            db.put("index".getBytes(StandardCharsets.UTF_8),
                    String.valueOf(version).getBytes(StandardCharsets.UTF_8));
        } catch (RocksDBException e) {
            logger.warn("saveIndexVersion failed", e);
        }
    }

    public int loadIndexVersion() {
        try {
            byte[] v = db.get("index".getBytes(StandardCharsets.UTF_8));
            return (v == null) ? 1 : Integer.parseInt(new String(v, StandardCharsets.UTF_8));
        } catch (Exception e) {
            logger.warn("loadIndexVersion failed", e);
            return 1;
        }
    }

    public List<String> getAllVectorIds() {
        List<String> out = new ArrayList<>();
        RocksIterator it = null;
        try {
            it = db.newIterator();
            for (it.seekToFirst(); it.isValid(); it.next()) {
                String key = new String(it.key(), StandardCharsets.UTF_8);
                if (!"index".equals(key)) {
                    out.add(key);
                }
            }
        } finally {
            if (it != null) {
                it.close();
            }
        }
        return out;
    }

    public void logStats() {
        try {
            logger.debug("rocksdb.stats:\n{}", db.getProperty("rocksdb.stats"));
        } catch (RocksDBException ignore) {}
    }

    public void printSummary() {
        try {
            logger.debug("estimate-num-keys={}", db.getLongProperty("rocksdb.estimate-num-keys"));
            logger.debug("num-live-sst-files={}", db.getProperty("rocksdb.num-live-sst-files"));
        } catch (RocksDBException ignore) {}
    }

    public String quickSummaryLine() {
        int version = loadIndexVersion();
        int metaKeys = getAllVectorIds().size();
        long files = 0L;
        try (Stream<Path> s = Files.walk(Paths.get(baseDir))) {
            files = s.filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".point"))
                    .count();
        } catch (IOException ignore) {}
        return String.format("RocksDB[%s] v=%d metaKeys=%d pointFiles=%d",
                dbPath, version, metaKeys, files);
    }

    public String getDbPath() {
        return dbPath;
    }

    public String getPointsBaseDir() {
        return baseDir;
    }

    public DriftReport auditDrift() {
        Set<String> idsInMeta = new HashSet<>(getAllVectorIds());

        Set<String> idsOnDisk = new HashSet<>();
        try (Stream<Path> s = Files.walk(Paths.get(baseDir))) {
            s.filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".point"))
                    .forEach(p -> idsOnDisk.add(stripExt(p.getFileName().toString())));
        } catch (IOException e) {
            logger.warn("auditDrift walk failed", e);
        }

        Set<String> onlyMeta = new HashSet<>(idsInMeta);
        onlyMeta.removeAll(idsOnDisk);
        Set<String> onlyDisk = new HashSet<>(idsOnDisk);
        onlyDisk.removeAll(idsInMeta);

        if (!onlyMeta.isEmpty() || !onlyDisk.isEmpty()) {
            logger.warn("Drift detected: onlyInMeta={}, onlyOnDisk={}", onlyMeta.size(), onlyDisk.size());
        } else {
            logger.info("Drift audit: OK ({} ids)", idsInMeta.size());
        }
        return new DriftReport(idsInMeta.size(), idsOnDisk.size(), onlyMeta, onlyDisk);
    }

    private static String stripExt(String name) {
        int i = name.lastIndexOf('.');
        return (i < 0) ? name : name.substring(0, i);
    }

    public static final class DriftReport {
        public final int metaCount, diskCount;
        public final Set<String> onlyMeta, onlyDisk;

        DriftReport(int metaCount, int diskCount, Set<String> onlyMeta, Set<String> onlyDisk) {
            this.metaCount = metaCount;
            this.diskCount = diskCount;
            this.onlyMeta = onlyMeta;
            this.onlyDisk = onlyDisk;
        }
    }

    /**
     * Get storage metrics tracker for this metadata manager.
     */
    public StorageMetrics getStorageMetrics() {
        return storageMetrics;
    }

    /**
     * Update storage metrics for a specific dimension.
     */
    public void updateDimensionStorage(int dim) {
        if (storageMetrics != null) {
            storageMetrics.updateDimensionStorage(dim);
        }
    }

    /**
     * Get current storage snapshot (cached, refreshed every 5 seconds).
     */
    public StorageMetrics.StorageSnapshot getStorageSnapshot() {
        if (storageMetrics == null) {
            return new StorageMetrics.StorageSnapshot(0L, 0L, 0L,
                    new ConcurrentHashMap<>(),
                    new ConcurrentHashMap<>());
        }
        return storageMetrics.getSnapshot();
    }

    // ------------------- serialization helpers -------------------

    private byte[] serializeMetadata(Map<String, String> m) {
        String s = m.entrySet().stream()
                .map(e -> escape(e.getKey()) + "=" + escape(e.getValue()))
                .collect(Collectors.joining(";"));
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private Map<String, String> deserializeMetadata(byte[] data) {
        String s = new String(data, StandardCharsets.UTF_8);
        return Arrays.stream(s.split("(?<!\\\\);"))
                .map(tok -> tok.split("(?<!\\\\)=", 2))
                .filter(kv -> kv.length == 2)
                .collect(Collectors.toMap(
                        kv -> unescape(kv[0]),
                        kv -> unescape(kv[1])
                ));
    }

    /**
     * Flush any pending writes in RocksDB to disk.
     * RocksDB doesn't have explicit flush, but accessing DB triggers durability.
     */
    public void flush() {
        try {
            if (this.db != null && !closed) {
                // Sync WAL to ensure durability
                this.db.syncWal();
                logger.debug("RocksDB WAL synced");
            }
        } catch (Exception e) {
            logger.warn("RocksDB flush/syncWal failed", e);
        }
    }

    /**
     * Returns total size of all .point files in the points base directory.
     */
    public long sizePointsDir() {
        try (var walk = Files.walk(Paths.get(baseDir))) {
            return walk
                    .filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".point"))
                    .mapToLong(p -> {
                        try {
                            return Files.size(p);
                        } catch (IOException ignore) {
                            return 0L;
                        }
                    })
                    .sum();
        } catch (IOException e) {
            return 0L;
        }
    }

    /**
     * Returns the version number for a given vector ID (or -1 if missing).
     */
    public int getVersionOfVector(String id) {
        Map<String, String> meta = getVectorMetadata(id);
        if (meta.isEmpty()) {
            return -1;
        }

        String v = meta.get("version");
        if (v == null) {
            return -1;
        }

        try {
            if (v.startsWith("v")) {
                v = v.substring(1);
            }
            return Integer.parseInt(v);
        } catch (Exception e) {
            return -1;
        }
    }

    private String escape(String in) {
        return in.replace("=", "\\=").replace(";", "\\;");
    }

    private String unescape(String in) {
        return in.replace("\\=", "=").replace("\\;", ";");
    }
}