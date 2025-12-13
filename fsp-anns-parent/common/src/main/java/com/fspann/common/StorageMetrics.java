package com.fspann.common;

import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Storage Metrics Tracker
 *
 * Responsibilities:
 *  - Track overall storage (all dimensions)
 *  - Track per-dimension storage breakdown
 *  - Track metadata storage
 *  - Track encrypted points storage
 *  - Track re-encryption storage deltas
 *  - Expose storage snapshots for profiling
 */
public final class StorageMetrics {
    private static final Logger logger = LoggerFactory.getLogger(StorageMetrics.class);

    private final Path pointsBaseDir;
    private final Path metaDBDir;

    // =====================================================
    // Overall storage (computed on demand)
    // =====================================================
    private volatile long cachedTotalBytes = 0L;
    private volatile long cachedMetaBytes = 0L;
    private volatile long cachedPointsBytes = 0L;
    private volatile long lastComputedNs = 0L;
    private static final long CACHE_TTL_NS = 5_000_000_000L;  // 5 seconds

    // =====================================================
    // Per-dimension tracking
    // =====================================================
    private final ConcurrentHashMap<Integer, DimensionStorageStats> dimStats =
            new ConcurrentHashMap<>();

    // =====================================================
    // Re-encryption tracking
    // =====================================================
    private final ConcurrentHashMap<Integer, ReencryptionStorageSnapshot> reencSnapshots =
            new ConcurrentHashMap<>();

    public StorageMetrics(Path pointsBaseDir, Path metaDBDir) {
        this.pointsBaseDir = pointsBaseDir;
        this.metaDBDir = metaDBDir;
    }

    // =====================================================
    // SNAPSHOT: Overall Storage Stats
    // =====================================================

    public StorageSnapshot getSnapshot() {
        long now = System.nanoTime();
        if (now - lastComputedNs > CACHE_TTL_NS) {
            recomputeStorage();
        }

        return new StorageSnapshot(
                cachedTotalBytes,
                cachedMetaBytes,
                cachedPointsBytes,
                new ConcurrentHashMap<>(dimStats),
                new ConcurrentHashMap<>(reencSnapshots)
        );
    }

    // =====================================================
    // COMPUTE: Force recomputation
    // =====================================================

    public void recomputeStorage() {
        long t0 = System.nanoTime();

        cachedMetaBytes = computeMetadataStorageBytes();
        cachedPointsBytes = computePointsStorageBytes();
        cachedTotalBytes = cachedMetaBytes + cachedPointsBytes;

        long dt = (System.nanoTime() - t0) / 1_000_000;
        lastComputedNs = System.nanoTime();

        logger.debug("Storage recomputed in {}ms: meta={} bytes, points={} bytes, total={} bytes",
                dt, cachedMetaBytes, cachedPointsBytes, cachedTotalBytes);
    }

    // =====================================================
    // COMPUTE: Metadata Storage
    // =====================================================

    private long computeMetadataStorageBytes() {
        if (!Files.exists(metaDBDir)) return 0L;

        try (Stream<Path> stream = Files.walk(metaDBDir)) {
            return stream
                    .filter(Files::isRegularFile)
                    .mapToLong(StorageMetrics::safeFileSize)
                    .sum();
        } catch (IOException e) {
            logger.warn("Failed to compute metadata storage", e);
            return 0L;
        }
    }

    // =====================================================
    // COMPUTE: Points Storage (overall)
    // =====================================================

    private long computePointsStorageBytes() {
        if (!Files.exists(pointsBaseDir)) return 0L;

        try (Stream<Path> stream = Files.walk(pointsBaseDir)) {
            return stream
                    .filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".point"))
                    .mapToLong(StorageMetrics::safeFileSize)
                    .sum();
        } catch (IOException e) {
            logger.warn("Failed to compute points storage", e);
            return 0L;
        }
    }

    // =====================================================
    // COMPUTE: Per-Dimension Storage
    // =====================================================

    public void updateDimensionStorage(int dim) {
        long bytes = computeDimensionStorageBytes(dim);
        dimStats.put(dim, new DimensionStorageStats(dim, bytes, System.currentTimeMillis()));
        logger.debug("Updated storage for dim={}: {} bytes", dim, bytes);
    }

    private long computeDimensionStorageBytes(int dim) {
        if (!Files.exists(pointsBaseDir)) return 0L;

        try (Stream<Path> stream = Files.walk(pointsBaseDir)) {
            return stream
                    .filter(Files::isDirectory)
                    .filter(p -> p.getFileName().toString().equals("v*"))  // version dirs
                    .flatMap(vdir -> {
                        try {
                            return Files.walk(vdir)
                                    .filter(Files::isRegularFile)
                                    .filter(f -> f.toString().endsWith(".point"));
                        } catch (IOException e) {
                            return Stream.empty();
                        }
                    })
                    .mapToLong(StorageMetrics::safeFileSize)
                    .sum();
        } catch (IOException e) {
            logger.warn("Failed to compute dimension {} storage", dim, e);
            return 0L;
        }
    }

    public DimensionStorageStats getDimensionStats(int dim) {
        DimensionStorageStats stats = dimStats.get(dim);
        if (stats == null) {
            updateDimensionStorage(dim);
            stats = dimStats.get(dim);
        }
        return stats != null ? stats : new DimensionStorageStats(dim, 0L, System.currentTimeMillis());
    }

    // =====================================================
    // RE-ENCRYPTION: Record Delta
    // =====================================================

    public void recordReencryptionSnapshot(int targetVersion,
                                           long bytesBeforeReenc,
                                           long bytesAfterReenc,
                                           int reencryptedCount,
                                           long timeMs) {
        long delta = bytesAfterReenc - bytesBeforeReenc;

        ReencryptionStorageSnapshot snap = new ReencryptionStorageSnapshot(
                targetVersion,
                System.currentTimeMillis(),
                bytesBeforeReenc,
                bytesAfterReenc,
                delta,
                reencryptedCount,
                timeMs
        );

        reencSnapshots.put(targetVersion, snap);

        logger.info("Re-encryption v{}: before={} bytes, after={} bytes, delta={} bytes, time={}ms",
                targetVersion, bytesBeforeReenc, bytesAfterReenc, delta, timeMs);
    }

    // =====================================================
    // HELPERS
    // =====================================================

    private static long safeFileSize(Path p) {
        try {
            return Files.size(p);
        } catch (IOException ignore) {
            return 0L;
        }
    }

    // =====================================================
    // DTOs
    // =====================================================

    /**
     * Overall storage snapshot at a point in time.
     */
    public static final class StorageSnapshot {
        public final long totalBytes;
        public final long metadataBytes;
        public final long pointsBytes;
        public final ConcurrentHashMap<Integer, DimensionStorageStats> perDimension;
        public final ConcurrentHashMap<Integer, ReencryptionStorageSnapshot> reencHistory;

        public StorageSnapshot(long totalBytes,
                               long metadataBytes,
                               long pointsBytes,
                               ConcurrentHashMap<Integer, DimensionStorageStats> perDimension,
                               ConcurrentHashMap<Integer, ReencryptionStorageSnapshot> reencHistory) {
            this.totalBytes = totalBytes;
            this.metadataBytes = metadataBytes;
            this.pointsBytes = pointsBytes;
            this.perDimension = new ConcurrentHashMap<>(perDimension);
            this.reencHistory = new ConcurrentHashMap<>(reencHistory);
        }

        public long getDimensionBytes(int dim) {
            DimensionStorageStats stats = perDimension.get(dim);
            return stats != null ? stats.bytes : 0L;
        }

        public String summary() {
            return String.format(
                    "Storage: total=%d KB, metadata=%d KB, points=%d KB, dimensions=%d",
                    totalBytes / 1024,
                    metadataBytes / 1024,
                    pointsBytes / 1024,
                    perDimension.size()
            );
        }
    }

    /**
     * Per-dimension storage statistics.
     */
    public static final class DimensionStorageStats {
        public final int dimension;
        public final long bytes;
        public final long computedAtMs;

        public DimensionStorageStats(int dimension, long bytes, long computedAtMs) {
            this.dimension = dimension;
            this.bytes = bytes;
            this.computedAtMs = computedAtMs;
        }

        @Override
        public String toString() {
            return String.format("Dim%d: %d bytes", dimension, bytes);
        }
    }

    /**
     * Re-encryption storage delta tracking.
     */
    public static final class ReencryptionStorageSnapshot {
        public final int targetVersion;
        public final long snapshotAtMs;
        public final long bytesBeforeReenc;
        public final long bytesAfterReenc;
        public final long bytesDelta;
        public final int reencryptedCount;
        public final long reencTimeMs;

        public ReencryptionStorageSnapshot(int targetVersion,
                                           long snapshotAtMs,
                                           long bytesBeforeReenc,
                                           long bytesAfterReenc,
                                           long bytesDelta,
                                           int reencryptedCount,
                                           long reencTimeMs) {
            this.targetVersion = targetVersion;
            this.snapshotAtMs = snapshotAtMs;
            this.bytesBeforeReenc = bytesBeforeReenc;
            this.bytesAfterReenc = bytesAfterReenc;
            this.bytesDelta = bytesDelta;
            this.reencryptedCount = reencryptedCount;
            this.reencTimeMs = reencTimeMs;
        }

        @Override
        public String toString() {
            return String.format(
                    "Re-enc v%d: before=%d KB, after=%d KB, delta=%d KB, reenc=%d, time=%d ms",
                    targetVersion,
                    bytesBeforeReenc / 1024,
                    bytesAfterReenc / 1024,
                    bytesDelta / 1024,
                    reencryptedCount,
                    reencTimeMs
            );
        }
    }

    // =====================================================
    // PUBLIC FAÇADE
    // =====================================================

    public long getTotalBytes() {
        return getSnapshot().totalBytes;
    }

    public long getMetadataBytes() {
        return getSnapshot().metadataBytes;
    }

    public long getPointsBytes() {
        return getSnapshot().pointsBytes;
    }

    public long getDimensionBytes(int dim) {
        return getSnapshot().getDimensionBytes(dim);
    }

    public String getSummary() {
        return getSnapshot().summary();
    }
}