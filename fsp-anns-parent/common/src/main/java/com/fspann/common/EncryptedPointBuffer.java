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
    private static final int FLUSH_THRESHOLD = 1000;

    private final Map<Integer, List<EncryptedPoint>> versionBuffer = new HashMap<>();
    private final Path baseDir;

    private final MetadataManager metadataManager;
    private final Map<Integer, Integer> batchCounters = new HashMap<>();

    public EncryptedPointBuffer(String baseDirPath, MetadataManager metadataManager) throws IOException {
        this.baseDir = Paths.get(baseDirPath);
        this.metadataManager = metadataManager;
        Files.createDirectories(baseDir);
    }

    public void add(EncryptedPoint pt) {
        versionBuffer.computeIfAbsent(pt.getVersion(), v -> new ArrayList<>()).add(pt);

        if (versionBuffer.get(pt.getVersion()).size() >= FLUSH_THRESHOLD) {
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

            logger.info("Flushed {} points for v{} to {}", points.size(), version, batchFileName);
        } catch (IOException e) {
            logger.error("Failed to flush EncryptedPoints for version {}", version, e);
        }

        batchCounters.put(version, batchCounters.getOrDefault(version, 0) + 1);
        versionBuffer.remove(version);
    }
}
