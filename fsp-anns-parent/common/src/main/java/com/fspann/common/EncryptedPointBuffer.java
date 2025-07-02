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
    private final Path baseDir;
    private final MetadataManager metadataManager;
    private static final double MEMORY_THRESHOLD_RATIO = 0.80;
    private final int flushThreshold;
    private int globalBufferCount = 0; // total points buffered across versions

    public EncryptedPointBuffer(String baseDirPath, MetadataManager metadataManager) throws IOException {
        this(baseDirPath, metadataManager, 10000); // default flush threshold
    }

    public EncryptedPointBuffer(String baseDirPath, MetadataManager metadataManager, int flushThreshold) throws IOException {
        this.baseDir = Paths.get(baseDirPath);
        this.metadataManager = metadataManager;
        this.flushThreshold = flushThreshold;
        Files.createDirectories(baseDir);
    }

    public synchronized void add(EncryptedPoint pt) {
        versionBuffer.computeIfAbsent(pt.getVersion(), v -> new ArrayList<>()).add(pt);
        globalBufferCount++;

        long maxMemory = Runtime.getRuntime().maxMemory();
        long usedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        if ((double) usedMemory / maxMemory > MEMORY_THRESHOLD_RATIO) {
            logger.warn("Memory usage exceeded 80%% of max ({} MB used of {} MB). Flushing buffers early.",
                    usedMemory / (1024 * 1024), maxMemory / (1024 * 1024));
            flushAll();
        }

        if (versionBuffer.get(pt.getVersion()).size() >= flushThreshold) {
            flush(pt.getVersion());
        }
    }

    public synchronized void flushAll() {
        for (Integer version : new ArrayList<>(versionBuffer.keySet())) {
            flush(version);
        }
    }

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
                metadataManager.putVectorMetadata(pt.getId(), String.valueOf(pt.getShardId()), String.valueOf(pt.getVersion()));
            }

//            logger.info("Flushed {} points for v{} to {}", points.size(), version, batchFileName);
        } catch (IOException e) {
//            logger.error("Failed to flush EncryptedPoints for version {}", version, e);
        }

        batchCounters.put(version, batchCounters.getOrDefault(version, 0) + 1);
        versionBuffer.remove(version);
    }

    public int getBufferSize() {
        return globalBufferCount;
    }

    public int getFlushThreshold() {
        return flushThreshold;
    }

    public void shutdown() {
        // No-op (executor removed)
    }
}
