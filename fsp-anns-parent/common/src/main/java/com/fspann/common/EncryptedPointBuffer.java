package com.fspann.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Optimized buffer for batching encrypted points before persistence.
 * Leverages the high-performance batch API to reduce IOPS at 100M scale.
 */
public class EncryptedPointBuffer {
    private static final Logger logger = LoggerFactory.getLogger(EncryptedPointBuffer.class);

    private final Map<Integer, List<EncryptedPoint>> versionBuffer = new HashMap<>();
    private final Path pointsDir;
    private final MetadataManager metadataManager; // Interface-based for Sharding support

    private static final double MEMORY_THRESHOLD_RATIO =
            Double.parseDouble(System.getProperty("buffer.mem.ratio", "0.75"));

    private final int flushThreshold;
    private int globalBufferCount = 0;
    private int totalFlushedPoints = 0;
    private long lastBatchInsertTimeMs = 0;

    public EncryptedPointBuffer(String pointsPath, MetadataManager metadataManager, int flushThreshold) throws IOException {
        Path p = Paths.get(pointsPath);
        this.pointsDir = p.isAbsolute() ? p.normalize() : p.toAbsolutePath().normalize();
        this.metadataManager = Objects.requireNonNull(metadataManager, "MetadataManager cannot be null");
        this.flushThreshold = flushThreshold;
        Files.createDirectories(pointsDir);
    }

    public synchronized void add(EncryptedPoint pt) {
        List<EncryptedPoint> bucket = versionBuffer.computeIfAbsent(
                pt.getVersion(), v -> new ArrayList<>(flushThreshold));

        bucket.add(pt);
        globalBufferCount++;

        // Rapid memory backpressure check
        if (globalBufferCount % 500 == 0) {
            checkMemoryPressure();
        }

        if (bucket.size() >= flushThreshold) {
            flush(pt.getVersion());
        }
    }

    private void checkMemoryPressure() {
        Runtime rt = Runtime.getRuntime();
        long used = rt.totalMemory() - rt.freeMemory();
        if ((double) used / rt.maxMemory() > MEMORY_THRESHOLD_RATIO) {
            logger.warn("JVM Memory Pressure Detected ({}/{} MB). Forcing Flush.",
                    used/1024/1024, rt.maxMemory()/1024/1024);
            flushAll();
        }
    }

    public synchronized void flushAll() {
        if (globalBufferCount == 0) return;
        logger.info("Emergency flush triggered: persisting {} buffered points.", globalBufferCount);
        new ArrayList<>(versionBuffer.keySet()).forEach(this::flush);
    }

    public synchronized void flush(int version) {
        long t0 = System.nanoTime();
        List<EncryptedPoint> points = versionBuffer.get(version);
        if (points == null || points.isEmpty()) return;

        int batchSize = points.size();

        try {
            // CONSOLIDATED IO: Metadata and Binary Data are saved in one optimized pass
            metadataManager.saveEncryptedPointsBatch(points);

            globalBufferCount -= batchSize;
            totalFlushedPoints += batchSize;
            versionBuffer.remove(version);

            lastBatchInsertTimeMs = (System.nanoTime() - t0) / 1_000_000;
            logger.info("Successfully flushed batch of {} points for v{} in {}ms",
                    batchSize, version, lastBatchInsertTimeMs);
        } catch (IOException e) {
            logger.error("FATAL: Batch flush failed for v{}. Data remains in memory to prevent loss.", version, e);
        }
    }

    public synchronized int getCount() { return globalBufferCount; }
    public synchronized int getTotalFlushed() { return totalFlushedPoints; }

    public void shutdown() {
        flushAll();
        logger.info("EncryptedPointBuffer shutdown complete. Total Flushed: {}", totalFlushedPoints);
    }
}