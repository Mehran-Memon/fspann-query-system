package com.fspann.common;

import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ShardedMetadataManager implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ShardedMetadataManager.class);

    private final int numShards;
    private final RocksDB[] shardDbs;
    private final Options[] shardOptions;
    private final Path[] shardPaths;
    private final String baseDir;
    private volatile boolean closed = false;

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

        // Initialize each shard
        for (int i = 0; i < numShards; i++) {
            Path shardPath = Paths.get(basePath, "shard_" + i);
            Files.createDirectories(shardPath);
            this.shardPaths[i] = shardPath;

            this.shardOptions[i] = new Options()
                    .setCreateIfMissing(true)
                    .setWriteBufferSize(16 * 1024 * 1024)
                    .setMaxBackgroundJobs(2)
                    .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
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
        int hash = vectorId.hashCode();
        return Math.abs(hash % numShards);
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

    /**
     * Save encrypted point (delegates to appropriate shard).
     */
    public void saveEncryptedPoint(EncryptedPoint pt) throws IOException {
        Objects.requireNonNull(pt, "pt");

        String safeVersion = "v" + pt.getVersion();
        Path versionDir = Paths.get(baseDir, safeVersion);
        Files.createDirectories(versionDir);

        Path tmp = versionDir.resolve(pt.getId() + ".point.tmp");
        Path dst = versionDir.resolve(pt.getId() + ".point");

        PersistenceUtils.saveObject(pt, tmp.toString(), baseDir);
        Files.move(tmp, dst, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

        Map<String, String> meta = new HashMap<>();
        meta.put("version", String.valueOf(pt.getVersion()));
        meta.put("shardId", String.valueOf(pt.getShardId()));
        meta.put("dim", String.valueOf(pt.getVectorLength()));

        updateVectorMetadata(pt.getId(), meta);
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
}
