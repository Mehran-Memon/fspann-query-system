package com.fspann.loader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class GroundtruthManager {
    private static final Logger logger = LoggerFactory.getLogger(GroundtruthManager.class);

    private List<int[]> rows = new ArrayList<>();

    /** Load groundtruth from .ivecs or .csv (delegates to DefaultDataLoader). */
    public void load(String path) throws IOException {
        DefaultDataLoader ddl = new DefaultDataLoader();
        List<int[]> loaded = ddl.loadGroundtruth(path);

        if (loaded == null || loaded.isEmpty()) {
            throw new IOException("Groundtruth file at " + path + " is empty or invalid");
        }

        // Sanitize rows
        this.rows = new ArrayList<>(loaded.size());
        int badRows = 0;
        for (int i = 0; i < loaded.size(); i++) {
            int[] row = loaded.get(i);
            if (row == null || row.length == 0) {
                badRows++;
                this.rows.add(new int[0]);
                continue;
            }
            // Deduplicate and sort for safety
            int[] sanitized = Arrays.stream(row).distinct().sorted().toArray();
            this.rows.add(sanitized);
        }

        if (badRows > 0) {
            logger.warn("GroundtruthManager.load: {} rows were empty/null in {}", badRows, path);
        }
        logger.info("GroundtruthManager loaded {} queries from {}", rows.size(), path);
    }

    /** Number of groundtruth rows (i.e., number of queries). */
    public int size() {
        return rows.size();
    }

    /** Return the full groundtruth row for a query (no truncation). */
    public int[] getGroundtruth(int queryIndex) {
        if (queryIndex < 0 || queryIndex >= rows.size()) return new int[0];
        return rows.get(queryIndex);
    }

    /** Return the top-K groundtruth ids for a query. */
    public int[] getGroundtruth(int queryIndex, int k) {
        if (queryIndex < 0 || queryIndex >= rows.size() || k <= 0) return new int[0];
        int[] full = rows.get(queryIndex);
        if (full == null || full.length == 0) return new int[0];
        return Arrays.copyOf(full, Math.min(k, full.length));
    }

    /** Verify that all groundtruth IDs fall within datasetSize. */
    public boolean isConsistentWithDatasetSize(int datasetSize) {
        if (datasetSize <= 0) return false;
        for (int[] row : rows) {
            for (int id : row) {
                if (id < 0 || id >= datasetSize) {
                    logger.error("Groundtruth ID {} is out of range [0, {})", id, datasetSize);
                    return false;
                }
            }
        }
        return true;
    }
}
