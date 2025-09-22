package com.fspann.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class EncryptedPointBuffer {
    private static final Logger logger = LoggerFactory.getLogger(EncryptedPointBuffer.class);

    // version -> pending points (flushed in batches)
    private final Map<Integer, List<EncryptedPoint>> versionBuffer = new HashMap<>();
    private final Map<Integer, Integer> batchCounters = new HashMap<>();

    private final Path pointsDir;
    private final RocksDBMetadataManager metadataManager;

    private static final double MEMORY_THRESHOLD_RATIO =
            Double.parseDouble(System.getProperty("buffer.mem.ratio", "0.80"));

    private final int flushThreshold;
    private int  globalBufferCount    = 0;
    private int  totalFlushedPoints   = 0;
    private long lastBatchInsertTimeMs = 0;

    public EncryptedPointBuffer(String pointsPath, RocksDBMetadataManager metadataManager) throws IOException {
        this(pointsPath, metadataManager, 1000);
    }

    public EncryptedPointBuffer(String pointsPath,
                                RocksDBMetadataManager metadataManager,
                                int flushThreshold) throws IOException {
        Path p = Paths.get(pointsPath);
        this.pointsDir = p.isAbsolute() ? p.normalize() : FsPaths.baseDir().resolve(p).normalize();
        this.metadataManager = Objects.requireNonNull(metadataManager, "MetadataManager cannot be null");
        this.flushThreshold  = flushThreshold;
        Files.createDirectories(pointsDir);
    }

    public synchronized void add(EncryptedPoint pt) {
        Objects.requireNonNull(pt, "EncryptedPoint cannot be null");

        // Create bucket for this key version if missing
        List<EncryptedPoint> bucket = versionBuffer.computeIfAbsent(
                pt.getVersion(), v -> new ArrayList<>(Math.max(1024, flushThreshold)));

        bucket.add(pt);
        globalBufferCount++;

        // Memory backpressure
        long maxMemory  = Runtime.getRuntime().maxMemory();
        long usedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        if ((double) usedMemory / maxMemory > MEMORY_THRESHOLD_RATIO) {
            logger.warn("Memory > {}% ({} MB / {} MB). Flushing all buffers.",
                    (int)(MEMORY_THRESHOLD_RATIO * 100),
                    usedMemory / (1024 * 1024), maxMemory / (1024 * 1024));
            flushAll(); // re-entrant safe (synchronized)
        }

        // Per-version flush threshold
        if (bucket.size() >= flushThreshold) {
            flush(pt.getVersion());
        }
    }

    public synchronized void flushAll() {
        if (globalBufferCount == 0) return;
        logger.info("Flushing all version buffers ({} total points)", globalBufferCount);
        // copy keys to avoid CME while flush(...) mutates the map
        for (Integer v : new ArrayList<>(versionBuffer.keySet())) {
            flush(v);
        }
    }

    public synchronized void flush(int version) {
        long t0 = System.nanoTime();

        List<EncryptedPoint> points = versionBuffer.get(version);
        if (points == null || points.isEmpty()) return;

        int flushedSize = points.size();
        int batchIndex  = batchCounters.getOrDefault(version, 0);
        String batchFileName = String.format("v%d_batch_%03d.points", version, batchIndex);
        Path versionDir = pointsDir.resolve("v" + version);

        try { Files.createDirectories(versionDir); }
        catch (IOException ioe) {
            logger.error("Failed to create version dir {}: {}", versionDir, ioe.toString());
            return;
        }

        // 1) Prepare metadata (dimension, version, shard and per-table buckets)
        Map<String, Map<String, String>> allMeta = new HashMap<>(flushedSize * 2);
        for (EncryptedPoint pt : points) {
            Map<String, String> meta = new HashMap<>();
            meta.put("version", String.valueOf(pt.getVersion()));
            meta.put("dim",     String.valueOf(pt.getVectorLength()));
            meta.put("shardId", String.valueOf(pt.getShardId()));
            List<Integer> buckets = pt.getBuckets();
            if (buckets != null) {
                for (int t = 0; t < buckets.size(); t++) {
                    meta.put("b" + t, String.valueOf(buckets.get(t)));
                }
            }
            allMeta.put(pt.getId(), meta);
        }

        // 2) Write metadata first (RocksDB)
        try {
            // Prefer the existing bulk API your code already uses elsewhere:
            metadataManager.batchUpdateVectorMetadata(allMeta);
        } catch (RuntimeException e) {
            logger.error("batchUpdateVectorMetadata failed for v{}, falling back per-point", version, e);
            for (var entry : allMeta.entrySet()) {
                try {
                    metadataManager.updateVectorMetadata(entry.getKey(), entry.getValue());
                } catch (RuntimeException ex) {
                    logger.error("updateVectorMetadata failed for id={}", entry.getKey(), ex);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // 3) Persist encrypted points (Rocks + sidecar file)
        //    Use a bulk call if your manager has one; otherwise per-point.
        try {
            // Bulk path (preferred, if available in your manager)
            // metadataManager.saveEncryptedPointsBulk(points);

            // Fallback: per-point
            for (EncryptedPoint pt : points) {
                metadataManager.saveEncryptedPoint(pt); // <-- ensures Rocks can restore later

                // Optional: keep sidecar on disk for audits/debugging (as you had)
                Path pointFile = versionDir.resolve(pt.getId() + ".point");
                PersistenceUtils.saveObject(pt, pointFile.toString(), pointsDir.toString());
            }
            logger.info("Flushed {} points to v{} ({})", flushedSize, version, batchFileName);
        } catch (Exception e) {
            logger.error("Failed to persist EncryptedPoints for v{}: {}", version, e.toString());
            // We keep the batch in memory if persistence failed; return to avoid dropping it.
            return;
        }

        // 4) Accounting + clear
        globalBufferCount     -= flushedSize;
        totalFlushedPoints    += flushedSize;
        batchCounters.put(version, batchIndex + 1);
        versionBuffer.remove(version);

        lastBatchInsertTimeMs = (System.nanoTime() - t0) / 1_000_000;
    }

    public int  getBufferSize()          { return globalBufferCount; }
    public int  getTotalFlushedPoints()  { return totalFlushedPoints; }
    public int  getFlushThreshold()      { return flushThreshold; }
    public long getLastBatchInsertTimeMs(){ return lastBatchInsertTimeMs; }

    public void shutdown() {
        flushAll();
        logger.info("EncryptedPointBuffer shutdown complete. totalFlushed={}", totalFlushedPoints);
    }
}
