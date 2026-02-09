package com.fspann.loader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

/**
 * GroundtruthManager - FIXED VERSION
 *
 * Changes:
 *   1. Direct IVECS parsing (no delegation to DefaultDataLoader)
 *   2. Validation that IDs are in expected range
 *   3. Logging of loaded data for verification
 */
public class GroundtruthManager {
    private static final Logger logger = LoggerFactory.getLogger(GroundtruthManager.class);

    private List<int[]> rows = new ArrayList<>();
    private int maxId = -1;
    private int minId = Integer.MAX_VALUE;

    /**
     * Load groundtruth from .ivecs file.
     *
     * IVECS format:
     *   - Each row starts with 4-byte int (k = number of neighbors)
     *   - Followed by k 4-byte ints (neighbor IDs)
     *   - Little-endian byte order
     */
    public void load(String path) throws IOException {
        Path p = Paths.get(path);

        if (!Files.exists(p)) {
            throw new IOException("Groundtruth file not found: " + path);
        }

        String lower = path.toLowerCase();
        if (lower.endsWith(".ivecs")) {
            loadIvecs(p);
        } else if (lower.endsWith(".csv")) {
            loadCsv(p);
        } else {
            throw new IOException("Unsupported groundtruth format: " + path +
                    " (expected .ivecs or .csv)");
        }

        // Log summary
        logger.info(
                "GroundtruthManager loaded: queries={}, k={}, ID range=[{}, {}]",
                rows.size(),
                rows.isEmpty() ? 0 : rows.get(0).length,
                minId,
                maxId
        );
    }

    /**
     * Load IVECS format directly.
     */
    private void loadIvecs(Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            long fileSize = channel.size();
            ByteBuffer intBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);

            // RE-USE this buffer for all rows
            ByteBuffer dataBuffer = ByteBuffer.allocate(1024 * 4);
            dataBuffer.order(ByteOrder.LITTLE_ENDIAN);

            long position = 0;
            while (position < fileSize) {
                intBuffer.clear();
                channel.read(intBuffer, position);
                intBuffer.flip();
                int k = intBuffer.getInt();
                position += 4;

                // Resize buffer only if K increases unexpectedly
                if (dataBuffer.capacity() < k * 4) {
                    dataBuffer = ByteBuffer.allocate(k * 4).order(ByteOrder.LITTLE_ENDIAN);
                }

                dataBuffer.clear();
                dataBuffer.limit(k * 4);
                channel.read(dataBuffer, position);
                dataBuffer.flip();

                int[] ids = new int[k];
                for (int i = 0; i < k; i++) {
                    ids[i] = dataBuffer.getInt();
                    if (ids[i] < minId) minId = ids[i];
                    if (ids[i] > maxId) maxId = ids[i];
                }
                rows.add(ids);
                position += (k * 4);
            }
        }
    }

    /**
     * Load CSV format (fallback).
     */
    private void loadCsv(Path path) throws IOException {
        List<String> lines = Files.readAllLines(path);

        this.rows = new ArrayList<>(lines.size());
        this.maxId = -1;
        this.minId = Integer.MAX_VALUE;

        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i).trim();
            if (line.isEmpty() || line.startsWith("#")) continue;

            String[] parts = line.split("[,\\s]+");
            int[] ids = new int[parts.length];

            for (int j = 0; j < parts.length; j++) {
                ids[j] = Integer.parseInt(parts[j].trim());
                if (ids[j] < minId) minId = ids[j];
                if (ids[j] > maxId) maxId = ids[j];
            }

            rows.add(ids);
        }

        if (rows.isEmpty()) {
            throw new IOException("No groundtruth rows loaded from CSV: " + path);
        }
    }

    /**
     * Number of groundtruth rows (queries).
     */
    public int size() {
        return rows.size();
    }

    /**
     * Get the full groundtruth row for a query (defensive copy).
     */
    public int[] getGroundtruth(int queryIndex) {
        if (queryIndex < 0 || queryIndex >= rows.size()) {
            logger.warn("GT query index out of range: {} (size={})", queryIndex, rows.size());
            return new int[0];
        }
        return rows.get(queryIndex).clone();
    }

    /**
     * Get the top-K groundtruth IDs for a query.
     */
    public int[] getGroundtruth(int queryIndex, int k) {
        if (queryIndex < 0 || queryIndex >= rows.size() || k <= 0) {
            return new int[0];
        }

        int[] full = rows.get(queryIndex);
        if (full == null || full.length == 0) return new int[0];

        int upto = Math.min(k, full.length);
        return (upto == full.length) ? full.clone() : Arrays.copyOf(full, upto);
    }

    /**
     * Alias for getGroundtruth(queryIndex, k).
     */
    public int[] getGroundtruthIds(int queryIndex, int k) {
        return getGroundtruth(queryIndex, k);
    }

    /**
     * Verify that all groundtruth IDs fall within the dataset size.
     */
    public boolean isConsistentWithDatasetSize(int datasetSize) {
        if (datasetSize <= 0) return false;

        if (maxId >= datasetSize) {
            logger.error(
                    "GT validation FAILED: maxId={} >= datasetSize={}",
                    maxId, datasetSize
            );
            return false;
        }

        if (minId < 0) {
            logger.error("GT validation FAILED: minId={} is negative", minId);
            return false;
        }

        logger.info(
                "GT validation PASSED: ID range [{}, {}] within [0, {})",
                minId, maxId, datasetSize
        );
        return true;
    }

    /**
     * Get the maximum ID found in groundtruth.
     */
    public int getMaxId() {
        return maxId;
    }

    /**
     * Get the minimum ID found in groundtruth.
     */
    public int getMinId() {
        return minId;
    }
}