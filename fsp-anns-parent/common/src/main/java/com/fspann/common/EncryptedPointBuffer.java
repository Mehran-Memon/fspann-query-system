package com.fspann.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class EncryptedPointBuffer {
    private static final Logger logger = LoggerFactory.getLogger(EncryptedPointBuffer.class);
    private final Map<Integer, List<EncryptedPoint>> versionBuffer = new HashMap<>();
    private final Map<Integer, Integer> batchCounters = new HashMap<>();
    private final Path pointsDir;
    private final RocksDBMetadataManager metadataManager;
    private static final double MEMORY_THRESHOLD_RATIO = 0.80;
    private final int flushThreshold;
    private int globalBufferCount = 0;
    private int totalFlushedPoints = 0;

    public EncryptedPointBuffer(String pointsPath, RocksDBMetadataManager metadataManager) throws IOException {
        this(pointsPath, metadataManager, 1000);
    }

    public EncryptedPointBuffer(String pointsPath, RocksDBMetadataManager metadataManager, int flushThreshold) throws IOException {
        this.pointsDir = Paths.get(pointsPath);
        this.metadataManager = Objects.requireNonNull(metadataManager, "MetadataManager cannot be null");
        this.flushThreshold = flushThreshold;
        Files.createDirectories(pointsDir);
    }

    public synchronized void add(EncryptedPoint pt) {
        Objects.requireNonNull(pt, "EncryptedPoint cannot be null");
        versionBuffer.computeIfAbsent(pt.getVersion(), v -> new ArrayList<>()).add(pt);
        globalBufferCount++;

        long maxMemory = Runtime.getRuntime().maxMemory();
        long usedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        double usageRatio = (double) usedMemory / maxMemory;

        if (usageRatio > MEMORY_THRESHOLD_RATIO) {
            logger.warn("Memory usage exceeded 80% ({} MB used of {} MB). Flushing all buffers.",
                    usedMemory / (1024 * 1024), maxMemory / (1024 * 1024));
            flushAll();
        }

        if (versionBuffer.get(pt.getVersion()).size() >= flushThreshold) {
            flush(pt.getVersion());
        }
    }

    public synchronized void flushAll() {
        logger.info("Flushing all version buffers ({} total points)", globalBufferCount);
        for (Integer version : new ArrayList<>(versionBuffer.keySet())) {
            flush(version);
        }
    }

    public synchronized void flush(int version) {
        List<EncryptedPoint> points = versionBuffer.get(version);
        if (points == null || points.isEmpty()) return;

        int flushedSize = points.size();
        int batchIndex = batchCounters.getOrDefault(version, 0);
        String batchFileName = String.format("v%d_batch_%03d.points", version, batchIndex);
        Path versionDir = pointsDir.resolve("v" + version);

        // Step 1: Prepare metadata
        Map<String, Map<String, String>> allMeta = new HashMap<>();
        for (EncryptedPoint pt : points) {
            allMeta.put(pt.getId(), Map.of(
                    "shardId", String.valueOf(pt.getShardId()),
                    "version", String.valueOf(pt.getVersion())
            ));
        }

        // Step 2: Try batch metadata update first
        try {
            metadataManager.batchPutMetadata(allMeta);
        } catch (RuntimeException e) {
            logger.error("Batch metadata update failed for v{}, falling back to individual updates", version, e);
            for (EncryptedPoint pt : points) {
                try {
                    metadataManager.updateVectorMetadata(pt.getId(), allMeta.get(pt.getId()));
                } catch (RuntimeException ex) {
                    logger.error("Failed to update metadata for point {}", pt.getId(), ex);
                }
            }
        }

        // Step 3: Save each point to disk
        try {
            Files.createDirectories(versionDir);
            for (EncryptedPoint pt : points) {
                Path pointFile = versionDir.resolve(pt.getId() + ".point");
                PersistenceUtils.saveObject(pt, pointFile.toString(), pointsDir.toString());
            }
            logger.info("Flushed {} points to disk (v{} - {})", flushedSize, version, batchFileName);
        } catch (IOException e) {
            logger.error("Failed to flush EncryptedPoints for version {} to {}: {}", version, batchFileName, e.getMessage());
        }

        globalBufferCount -= flushedSize;
        totalFlushedPoints += flushedSize;
        batchCounters.put(version, batchIndex + 1);
        versionBuffer.remove(version);
    }

    public int getBufferSize() {
        return globalBufferCount;
    }

    public int getTotalFlushedPoints() {
        return totalFlushedPoints;
    }

    public int getFlushThreshold() {
        return flushThreshold;
    }

    public void shutdown() {
        flushAll();
        logger.info("EncryptedPointBuffer shutdown complete, total flushed: {}", totalFlushedPoints);
    }
}
