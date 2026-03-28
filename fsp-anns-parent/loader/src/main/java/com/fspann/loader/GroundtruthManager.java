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

            if (fileSize == 0) {
                throw new IOException("Groundtruth file is empty: " + path);
            }

            // Read file in chunks
            ByteBuffer buffer = ByteBuffer.allocate(4);
            buffer.order(ByteOrder.LITTLE_ENDIAN);

            this.rows = new ArrayList<>();
            this.maxId = -1;
            this.minId = Integer.MAX_VALUE;

            long position = 0;
            int rowIndex = 0;

            while (position < fileSize) {
                // Read k (number of neighbors)
                buffer.clear();
                int bytesRead = channel.read(buffer, position);
                if (bytesRead < 4) break;

                buffer.flip();
                int k = buffer.getInt();

                if (k <= 0 || k > 100000) {
                    throw new IOException(
                            String.format("Invalid k=%d at row %d position %d", k, rowIndex, position)
                    );
                }

                position += 4;

                // Read neighbor IDs
                ByteBuffer dataBuffer = ByteBuffer.allocate(k * 4);
                dataBuffer.order(ByteOrder.LITTLE_ENDIAN);

                bytesRead = channel.read(dataBuffer, position);
                if (bytesRead < k * 4) {
                    throw new IOException(
                            String.format("Truncated row %d: expected %d bytes, got %d",
                                    rowIndex, k * 4, bytesRead)
                    );
                }

                dataBuffer.flip();
                int[] ids = new int[k];
                for (int i = 0; i < k; i++) {
                    ids[i] = dataBuffer.getInt();
                    if (ids[i] < minId) minId = ids[i];
                    if (ids[i] > maxId) maxId = ids[i];
                }

                rows.add(ids);
                position += k * 4;
                rowIndex++;
            }

            if (rows.isEmpty()) {
                throw new IOException("No groundtruth rows loaded from: " + path);
            }

            // Log first few rows for debugging
            if (logger.isDebugEnabled()) {
                for (int i = 0; i < Math.min(3, rows.size()); i++) {
                    int[] row = rows.get(i);
                    logger.debug("GT row {}: k={}, first5={}",
                            i, row.length,
                            Arrays.stream(row).limit(5).mapToObj(Integer::toString)
                                    .reduce((a, b) -> a + "," + b).orElse("")
                    );
                }
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