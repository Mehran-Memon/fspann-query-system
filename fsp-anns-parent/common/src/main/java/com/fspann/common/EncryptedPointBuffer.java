package com.fspann.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Buffered write handler for EncryptedPoint objects, with flush-on-threshold
 * and RocksDB-backed metadata persistence to minimize memory usage.
 */
public class EncryptedPointBuffer {
    private static final Logger logger = LoggerFactory.getLogger(EncryptedPointBuffer.class);

    private final Map<Integer, List<EncryptedPoint>> versionBuffer = new HashMap<>();
    private final Map<Integer, Integer> batchCounters = new HashMap<>();
    private final Path baseDir;
    private final RocksDBMetadataManager metadataManager;

    private static final double MEMORY_THRESHOLD_RATIO = 0.80;
    private final int flushThreshold;
    private int globalBufferCount = 0; // total points buffered across all versions

    /**
     * Constructor with default flush threshold (1000).
     */
    public EncryptedPointBuffer(String baseDirPath, RocksDBMetadataManager metadataManager) throws IOException {
        this(baseDirPath, metadataManager, 1000);
    }

    /**
     * Constructor with custom flush threshold.
     */
    public EncryptedPointBuffer(String baseDirPath, RocksDBMetadataManager metadataManager, int flushThreshold) throws IOException {
        this.baseDir = Paths.get(baseDirPath);
        this.metadataManager = metadataManager;
        this.flushThreshold = flushThreshold;
        Files.createDirectories(baseDir);
    }

    /**
     * Adds an EncryptedPoint to the buffer and triggers flush if necessary.
     */
    public synchronized void add(EncryptedPoint pt) {
        versionBuffer.computeIfAbsent(pt.getVersion(), v -> new ArrayList<>()).add(pt);
        globalBufferCount++;

        // Trigger early flush if memory usage is too high
        long maxMemory = Runtime.getRuntime().maxMemory();
        long usedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        if ((double) usedMemory / maxMemory > MEMORY_THRESHOLD_RATIO) {
            logger.warn("Memory usage exceeded 80%% of max ({} MB used of {} MB). Flushing buffers early.",
                    usedMemory / (1024 * 1024), maxMemory / (1024 * 1024));
            flushAll();
        }

        // Flush if version-specific buffer exceeds threshold
        if (versionBuffer.get(pt.getVersion()).size() >= flushThreshold) {
            flush(pt.getVersion());
        }
    }

    /**
     * Flushes all buffered points across all versions.
     */
    public synchronized void flushAll() {
        for (Integer version : new ArrayList<>(versionBuffer.keySet())) {
            flush(version);
        }
    }

    /**
     * Flushes points for a specific version to disk and stores metadata.
     */
    public synchronized void flush(int version) {
        List<EncryptedPoint> points = versionBuffer.getOrDefault(version, Collections.emptyList());
        if (points.isEmpty()) return;

        String batchFileName = String.format("v%d_batch_%03d.points", version, batchCounters.getOrDefault(version, 0));
        Path versionDir = baseDir.resolve("v" + version);
        Path batchFile = versionDir.resolve(batchFileName);

        try {
            Files.createDirectories(versionDir);
            try (OutputStream fos = Files.newOutputStream(batchFile);
                 ObjectOutputStream oos = new ObjectOutputStream(fos)) {
                oos.writeObject(points);
            }

            for (EncryptedPoint pt : points) {
                Map<String, String> meta = new HashMap<>();
                meta.put("shardId", String.valueOf(pt.getShardId()));
                meta.put("version", String.valueOf(pt.getVersion()));
                metadataManager.putVectorMetadata(pt.getId(), meta);
            }

            logger.info("Flushed {} points for v{} to {}", points.size(), version, batchFileName);
        } catch (IOException e) {
            logger.error("Failed to flush EncryptedPoints for version {}", version, e);
        }

        globalBufferCount -= points.size(); // Update global count
        batchCounters.put(version, batchCounters.getOrDefault(version, 0) + 1);
        versionBuffer.remove(version);
    }

    /**
     * Returns current buffer size across all versions.
     */
    public int getBufferSize() {
        return globalBufferCount;
    }

    /**
     * Returns configured flush threshold.
     */
    public int getFlushThreshold() {
        return flushThreshold;
    }

    /**
     * Cleanup resources, Currently a no-op.
     */
    public void shutdown() {
        // RocksDB will be closed elsewhere via shutdown hook.
    }
}