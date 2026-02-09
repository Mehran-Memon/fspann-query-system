package com.fspann.common;

import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ShardedMetadataManager implements MetadataManager {
    private static final Logger logger = LoggerFactory.getLogger(ShardedMetadataManager.class);

    private final int numShards;
    private final RocksDB[] shardDbs;
    private final Options[] shardOptions;
    private final Path[] shardPaths;
    private final String baseDir;
    private volatile boolean closed = false;
    protected static final int POINTS_PER_FILE = 1000;
    private final ConcurrentHashMap<Path, Object> batchLocks = new ConcurrentHashMap<>();
    private boolean syncWrites = true;
    private final StorageMetrics storageMetrics;

    static {
        try {
            RocksDB.loadLibrary();
            logger.info("RocksDB native library loaded for ShardedMetadataManager");
        } catch (Throwable t) {
            throw new RuntimeException("Failed to load RocksDB native library", t);
        }
    }

    /**
     * ShardRouter constructor to initialize shards based on the number of shards
     */
    public ShardedMetadataManager(String basePath, int numShards, String baseDir) throws IOException {
        if (numShards <= 0) {
            throw new IllegalArgumentException("numShards must be > 0");
        }

        this.numShards = numShards;
        this.baseDir = baseDir;
        this.shardDbs = new RocksDB[numShards];
        this.shardOptions = new Options[numShards];
        this.shardPaths = new Path[numShards];

        this.storageMetrics = new StorageMetrics(Paths.get(baseDir), Paths.get(basePath));

        for (int i = 0; i < numShards; i++) {
            Path shardPath = Paths.get(basePath, "shard_" + i);
            Files.createDirectories(shardPath);
            this.shardPaths[i] = shardPath;

            this.shardOptions[i] = new Options()
                    .setCreateIfMissing(true)
                    // 64MB is the sweet spot for 16 shards on 384GB RAM
                    .setWriteBufferSize(64 * 1024 * 1024)
                    .setMaxWriteBufferNumber(4)
                    .setMinWriteBufferNumberToMerge(1)

                    // Performance optimizations for massive ingestion
                    .setMaxBackgroundJobs(4)
                    .setLevel0FileNumCompactionTrigger(4)
                    .setLevel0SlowdownWritesTrigger(20)
                    .setLevel0StopWritesTrigger(30)

                    // Critical for 100M: Prevent OS IO-stall by syncing in small chunks
                    .setBytesPerSync(1024 * 1024)

                    .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
                    .setMaxOpenFiles(10000)
                    .setInfoLogLevel(InfoLogLevel.WARN_LEVEL);

            try {
                this.shardDbs[i] = RocksDB.open(this.shardOptions[i], shardPath.toString());
            } catch (RocksDBException e) {
                for (int j = 0; j < i; j++) {
                    try { shardDbs[j].close(); } catch (Exception ignore) {}
                    try { shardOptions[j].close(); } catch (Exception ignore) {}
                }
                throw new IOException("Failed to open RocksDB shard " + i, e);
            }
        }

        logger.info("ShardedMetadataManager initialized: {} shards at {}", numShards, basePath);
    }

    /**
     * Determine which shard a vector ID belongs to.
     */
    private int getShardIndex(String vectorId) {
        // Hash the vector ID and apply modulo operation for even distribution
        int hash = vectorId.hashCode();
        return Math.abs(hash) % numShards; // Ensure positive shard index
    }

    public void batchUpdateMetadata(Collection<EncryptedPoint> points) throws IOException {
        if (points == null || points.isEmpty()) return;

        // Group the metadata updates by shard
        Map<Integer, Map<String, Map<String, String>>> shardMetadataUpdates = new HashMap<>();

        for (EncryptedPoint pt : points) {
            String vectorId = pt.getId();
            Map<String, String> metadata = new HashMap<>();
            metadata.put("version", String.valueOf(pt.getVersion()));
            metadata.put("shardId", String.valueOf(pt.getShardId()));
            metadata.put("dim", String.valueOf(pt.getVectorLength()));
            metadata.put("batchFile", String.format("batch_%08d.dat", pt.getBatchId()));
            metadata.put("offsetInBatch", String.valueOf(pt.getOffsetInBatch()));

            // Determine the shard based on vectorId
            int shardIndex = getShardIndex(vectorId);

            // Add metadata to the corresponding shard group
            shardMetadataUpdates
                    .computeIfAbsent(shardIndex, k -> new HashMap<>())
                    .put(vectorId, metadata);
        }

        // Perform batch update for each shard
        for (Map.Entry<Integer, Map<String, Map<String, String>>> shardEntry : shardMetadataUpdates.entrySet()) {
            int shard = shardEntry.getKey();
            Map<String, Map<String, String>> updates = shardEntry.getValue();

            try (WriteBatch batch = new WriteBatch(); WriteOptions writeOptions = new WriteOptions().setSync(syncWrites)) {
                for (Map.Entry<String, Map<String, String>> entry : updates.entrySet()) {
                    batch.put(entry.getKey().getBytes(StandardCharsets.UTF_8), serializeMetadata(entry.getValue()));
                }
                // Write batch to the corresponding shard's RocksDB
                shardDbs[shard].write(writeOptions, batch);
            } catch (RocksDBException e) {
                throw new IOException("Batch update failed for shard " + shard, e);
            }
        }
    }


    /**
     * Get metadata for a single vector.
     */
    public synchronized Map<String, String> getVectorMetadata(String vectorId) {
        Objects.requireNonNull(vectorId, "vectorId");

        int shard = getShardIndex(vectorId);
        try {
            byte[] v = shardDbs[shard].get(vectorId.getBytes(StandardCharsets.UTF_8));
            return (v == null) ? Collections.emptyMap() : deserializeMetadata(v);
        } catch (RocksDBException e) {
            logger.warn("getVectorMetadata failed for {} (shard={})", vectorId, shard, e);
            return Collections.emptyMap();
        }
    }

    /**
     * Update metadata for a single vector.
     */
    public synchronized void updateVectorMetadata(String vectorId, Map<String, String> updates) {
        Objects.requireNonNull(vectorId, "vectorId");
        Objects.requireNonNull(updates, "updates");

        int shard = getShardIndex(vectorId);
        try {
            shardDbs[shard].put(vectorId.getBytes(StandardCharsets.UTF_8), serializeMetadata(updates));
        } catch (RocksDBException e) {
            throw new RuntimeException("updateVectorMetadata failed for " + vectorId + " (shard=" + shard + ")", e);
        }
    }

    /**
     * Batch update: distributes updates across shards.
     */
    public synchronized void batchUpdateVectorMetadata(Map<String, Map<String, String>> updates) throws IOException {
        Objects.requireNonNull(updates, "updates");

        Map<Integer, List<Map.Entry<String, Map<String, String>>>> shardGroups = new HashMap<>();

        for (Map.Entry<String, Map<String, String>> e : updates.entrySet()) {
            int shard = getShardIndex(e.getKey());
            shardGroups.computeIfAbsent(shard, k -> new ArrayList<>()).add(e);
        }

        for (Map.Entry<Integer, List<Map.Entry<String, Map<String, String>>>> entry : shardGroups.entrySet()) {
            int shard = entry.getKey();
            List<Map.Entry<String, Map<String, String>>> entries = entry.getValue();

            try (WriteBatch batch = new WriteBatch(); WriteOptions wo = new WriteOptions()) {
                for (Map.Entry<String, Map<String, String>> e : entries) {
                    batch.put(e.getKey().getBytes(StandardCharsets.UTF_8), serializeMetadata(e.getValue()));
                }
                shardDbs[shard].write(wo, batch);
            } catch (RocksDBException e) {
                throw new IOException("Batch write failed for shard " + shard, e);
            }
        }
    }


    @Override
    public void saveEncryptedPoint(EncryptedPoint pt) throws IOException {
        Objects.requireNonNull(pt, "pt");

        // 1. Precise ID Parsing
        long pointId;
        try {
            pointId = Long.parseLong(pt.getId().replaceAll("[^0-9]", ""));
        } catch (Exception e) { pointId = 0; }

        long batchId = pointId / POINTS_PER_FILE;
        String safeVersion = "v" + pt.getVersion();
        Path batchFile = Paths.get(baseDir, safeVersion, String.format("batch_%08d.dat", batchId));

        long fileOffset = 0;
        int serializedSize = 0;

        // 2. Lock-Protected Byte Offset Calculation & Physical Write
        synchronized (getBatchLock(batchFile)) {
            Files.createDirectories(batchFile.getParent());
            File f = batchFile.toFile();
            if (f.exists()) fileOffset = f.length();

            try (FileOutputStream fos = new FileOutputStream(f, true);
                 BufferedOutputStream bos = new BufferedOutputStream(fos);
                 DataOutputStream dos = new DataOutputStream(bos)) {

                byte[] serialized = PersistenceUtils.serializePoint(pt);
                serializedSize = serialized.length;

                dos.writeInt(serializedSize);
                dos.write(serialized);
                dos.flush();
            }
        }

        // --- NEW: INCREMENTAL STORAGE TRACKING ---
        if (this.storageMetrics != null) {
            // Increment points counter: Data length + 4 bytes for the Int header
            storageMetrics.addPointsBytes(serializedSize + 4);
            // Estimate metadata growth in RocksDB (~200 bytes per record)
            storageMetrics.addMetaBytes(200);
        }

        // 3. Shard-Aware Metadata Update
        Map<String, String> meta = new HashMap<>();
        meta.put("version", String.valueOf(pt.getVersion()));
        meta.put("batchFile", batchFile.getFileName().toString());
        meta.put("fileOffset", String.valueOf(fileOffset));
        meta.put("shardId", String.valueOf(pt.getShardId()));
        meta.put("dim", String.valueOf(pt.getVectorLength()));

        updateVectorMetadata(pt.getId(), meta);
    }

    @Override
    public EncryptedPoint loadEncryptedPoint(String id) throws IOException, ClassNotFoundException {
        Objects.requireNonNull(id, "id");
        Map<String, String> meta = getVectorMetadata(id);

        if (meta.isEmpty() || !meta.containsKey("batchFile") || !meta.containsKey("fileOffset")) {
            return null; // or fallback to legacy .point logic if needed
        }

        String ver = meta.get("version");
        String safeVersion = ver.startsWith("v") ? ver : "v" + ver;
        String batchFileName = meta.get("batchFile");
        long fileOffset = Long.parseLong(meta.get("fileOffset"));

        Path batchPath = Paths.get(baseDir, safeVersion, batchFileName);
        if (!Files.exists(batchPath)) return null;

        // USE RANDOM ACCESS (Same as monolithic fix)
        try (RandomAccessFile raf = new RandomAccessFile(batchPath.toFile(), "r")) {
            raf.seek(fileOffset);
            int len = raf.readInt();
            byte[] data = new byte[len];
            raf.readFully(data);
            return PersistenceUtils.deserializePoint(data);
        }
    }

    @Override
    public void saveEncryptedPointsBatch(Collection<EncryptedPoint> points) throws IOException {
        if (points == null || points.isEmpty()) return;

        // Map to store final metadata for all points in this batch
        Map<String, Map<String, String>> finalMetadataUpdates = new HashMap<>();
        long totalBatchBytesWritten = 0;

        // Group by batch file
        Map<Path, List<EncryptedPoint>> batchGroups = new HashMap<>();
        for (EncryptedPoint pt : points) {
            long pointId = Long.parseLong(pt.getId().replaceAll("[^0-9]", ""));
            long batchId = pointId / POINTS_PER_FILE;
            Path batchFile = Paths.get(baseDir, "v" + pt.getVersion(), String.format("batch_%08d.dat", batchId));
            batchGroups.computeIfAbsent(batchFile, k -> new ArrayList<>()).add(pt);
        }

        // Write groups and track offsets
        for (var entry : batchGroups.entrySet()) {
            Path batchFile = entry.getKey();
            synchronized (getBatchLock(batchFile)) {
                Files.createDirectories(batchFile.getParent());
                File f = batchFile.toFile();

                // Re-open in append mode
                try (FileOutputStream fos = new FileOutputStream(f, true);
                     BufferedOutputStream bos = new BufferedOutputStream(fos);
                     DataOutputStream dos = new DataOutputStream(bos)) {

                    long currentOffset = f.length();

                    for (EncryptedPoint pt : entry.getValue()) {
                        byte[] serialized = PersistenceUtils.serializePoint(pt);
                        int recordLen = serialized.length;

                        // Store the metadata mapping for this point
                        Map<String, String> m = new HashMap<>();
                        m.put("version", String.valueOf(pt.getVersion()));
                        m.put("batchFile", batchFile.getFileName().toString());
                        m.put("fileOffset", String.valueOf(currentOffset)); // CRITICAL: Byte Offset
                        m.put("shardId", String.valueOf(pt.getShardId()));
                        m.put("dim", String.valueOf(pt.getVectorLength()));
                        finalMetadataUpdates.put(pt.getId(), m);

                        dos.writeInt(recordLen);
                        dos.write(serialized);

                        // Accounting for this file
                        int totalWrittenForThisPoint = 4 + recordLen;
                        currentOffset += totalWrittenForThisPoint;
                        totalBatchBytesWritten += totalWrittenForThisPoint;
                    }
                    dos.flush();
                }
            }
        }

        // --- NEW: INCREMENTAL STORAGE TRACKING ---
        if (this.storageMetrics != null) {
            // Add total binary data size to points counter
            storageMetrics.addPointsBytes(totalBatchBytesWritten);
            // Add estimated RocksDB growth (batch size * ~200 bytes) to meta counter
            storageMetrics.addMetaBytes(points.size() * 200L);
        }

        // Update RocksDB shards with the calculated offsets in a single WriteBatch
        batchUpdateVectorMetadata(finalMetadataUpdates);
    }

    private Object getBatchLock(Path batchFile) {
        return batchLocks.computeIfAbsent(batchFile, k -> new Object());
    }

    @Override
    public void putVectorMetadata(String vectorId, Map<String, String> metadataMap) {
        updateVectorMetadata(vectorId, metadataMap);
    }

    /**
     * Get all encrypted points (needed for re-encryption)
     */
    public List<EncryptedPoint> getAllEncryptedPoints() {
        List<EncryptedPoint> list = new ArrayList<>();
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

        logger.info("Loaded {} encrypted points from {} shards", list.size(), numShards);
        return list;
    }


    @Override
    public StorageMetrics getStorageMetrics() {
        return this.storageMetrics;
    }

    @Override
    public void saveIndexVersion(int version) {

    }

    @Override
    public void printSummary() {

    }

    @Override
    public void logStats() {

    }

    /**
     * Flush all shards (ensure durability)
     */
    public void flush() {
        for (int i = 0; i < numShards; i++) {
            try {
                if (shardDbs[i] != null && !closed) {
                    shardDbs[i].syncWal();
                }
            } catch (Exception e) {
                logger.warn("Flush failed for shard {}", i, e);
            }
        }
        logger.debug("All {} shards flushed", numShards);
    }

    /**
     * Get all vector IDs across all shards.
     */
    public List<String> getAllVectorIds() {
        List<String> allIds = new ArrayList<>();

        for (int i = 0; i < numShards; i++) {
            RocksIterator it = null;
            try {
                it = shardDbs[i].newIterator();
                for (it.seekToFirst(); it.isValid(); it.next()) {
                    String key = new String(it.key(), StandardCharsets.UTF_8);
                    if (!"index".equals(key)) {
                        allIds.add(key);
                    }
                }
            } finally {
                if (it != null) it.close();
            }
        }

        return allIds;
    }

    @Override
    public void close() {
        synchronized (this) {
            if (closed) return;

            for (int i = 0; i < numShards; i++) {
                try {
                    if (shardDbs[i] != null) {
                        shardDbs[i].syncWal();
                        shardDbs[i].close();
                    }
                } catch (Throwable t) {
                    logger.warn("Error closing shard {} DB", i, t);
                } finally {
                    try {
                        if (shardOptions[i] != null) {
                            shardOptions[i].close();
                        }
                    } catch (Throwable ignore) {
                    }
                }
            }
            closed = true;
            logger.info("ShardedMetadataManager closed ({} shards)", numShards);
        }
    }

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

    private String escape(String in) {
        return in.replace("=", "\\=").replace(";", "\\;");
    }

    private String unescape(String in) {
        return in.replace("\\=", "=").replace("\\;", ";");
    }


    @Override
    public long sizePointsDir() {
        try (Stream<Path> walk = Files.walk(Paths.get(baseDir))) {
            return walk.filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".point") || p.toString().endsWith(".dat"))
                    .mapToLong(p -> {
                        try { return Files.size(p); } catch (IOException e) { return 0L; }
                    }).sum();
        } catch (IOException e) { return 0L; }
    }

    @Override
    public int countWithVersion(int keyVersion) throws IOException {
        int total = 0;
        // We aggregate the count from every shard
        for (int i = 0; i < numShards; i++) {
            try (RocksIterator it = shardDbs[i].newIterator()) {
                for (it.seekToFirst(); it.isValid(); it.next()) {
                    Map<String, String> meta = deserializeMetadata(it.value());
                    String v = meta.get("version");
                    if (v != null && Integer.parseInt(v.replaceAll("[^0-9]", "")) == keyVersion) {
                        total++;
                    }
                }
            }
        }
        return total;
    }

    @Override
    public int getVersionOfVector(String id) {
        Map<String, String> meta = getVectorMetadata(id);
        if (meta.isEmpty()) return -1;
        String v = meta.get("version");
        try {
            return (v == null) ? -1 : Integer.parseInt(v.replaceAll("[^0-9]", ""));
        } catch (Exception e) { return -1; }
    }

    @Override
    public boolean isDeleted(String vectorId) {
        Objects.requireNonNull(vectorId, "vectorId cannot be null");

        // Route to the correct shard
        Map<String, String> meta = getVectorMetadata(vectorId);
        if (meta == null || meta.isEmpty()) {
            return false;
        }

        String deletedFlag = meta.getOrDefault("deleted", "false");
        return "true".equalsIgnoreCase(deletedFlag);
    }

    @Override
    public long getDeletedTimestamp(String vectorId) {
        Map<String, String> meta = getVectorMetadata(vectorId);
        if (meta == null) return -1L;

        String timestampStr = meta.get("deleted_at");
        try {
            return (timestampStr != null) ? Long.parseLong(timestampStr) : -1L;
        } catch (NumberFormatException e) {
            return -1L;
        }
    }

    @Override
    public void hardDeleteVector(String vectorId) {
        if (!isDeleted(vectorId)) return;

        int shard = Math.abs(vectorId.hashCode()) % numShards;
        try {
            shardDbs[shard].delete(vectorId.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        } catch (org.rocksdb.RocksDBException e) {
            logger.error("Failed to hard delete vector {} from shard {}", vectorId, shard, e);
        }
    }

    @Override
    public List<String> getIdsFromShard(int shardIndex, int limit) {
        if (shardIndex < 0 || shardIndex >= numShards) {
            return Collections.emptyList();
        }

        List<String> ids = new ArrayList<>(limit);
        // Use a try-with-resources to ensure the iterator is closed
        try (RocksIterator it = shardDbs[shardIndex].newIterator()) {
            it.seekToFirst();
            while (it.isValid() && ids.size() < limit) {
                String key = new String(it.key(), StandardCharsets.UTF_8);
                if (!"index".equals(key)) {
                    ids.add(key);
                }
                it.next();
            }
        }
        return ids;
    }

}
