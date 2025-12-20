package com.fspann.loader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class GroundtruthManager {
    private static final Logger logger = LoggerFactory.getLogger(GroundtruthManager.class);

    private List<int[]> rows = new ArrayList<>();
    private boolean normalized = false;
    private boolean convertedFromOneBased = false;

    /** Load groundtruth from .ivecs or .csv (delegates to DefaultDataLoader). */
    public void load(String path) throws IOException {
        DefaultDataLoader ddl = new DefaultDataLoader();
        // Read all rows via the new streaming-based loader
        List<int[]> loaded = ddl.loadGroundtruthAll(path);

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
            this.rows.add(row.clone());
        }

        if (badRows > 0) {
            logger.warn("GroundtruthManager.load: {} rows were empty/null in {}", badRows, path);
        }
        logger.info("GroundtruthManager loaded {} queries from {}", rows.size(), path);
        normalized = false;
        convertedFromOneBased = false;
    }

    /** Number of groundtruth rows (i.e., number of queries). */
    public int size() { return rows.size(); }

    /** Return the full groundtruth row for a query (defensive copy). */
    public int[] getGroundtruth(int queryIndex) {
        if (queryIndex < 0 || queryIndex >= rows.size()) return new int[0];
        return rows.get(queryIndex).clone();
    }

    /** Return the top-K groundtruth ids for a query. */
    public int[] getGroundtruth(int queryIndex, int k) {
        if (queryIndex < 0 || queryIndex >= rows.size() || k <= 0) return new int[0];
        int[] full = rows.get(queryIndex);
        if (full == null || full.length == 0) return new int[0];
        int upto = Math.min(k, full.length);
        return (upto == full.length) ? full.clone() : Arrays.copyOf(full, upto);
    }

    /** Verify that all groundtruth IDs fall within datasetSize. */
    public boolean isConsistentWithDatasetSize(int datasetSize) {
        if (datasetSize <= 0) return false;
        long oor = 0L;
        for (int[] row : rows) {
            if (row == null) continue;
            for (int id : row) {
                if (id < 0 || id >= datasetSize) oor++;
            }
        }
        if (oor > 0) {
            logger.error("Groundtruth has {} out-of-range ids; datasetSize={}", oor, datasetSize);
            return false;
        }
        return true;
    }

    public int[] getGroundtruthIds(int q, int k) {
        return getGroundtruth(q, k);
    }
}
