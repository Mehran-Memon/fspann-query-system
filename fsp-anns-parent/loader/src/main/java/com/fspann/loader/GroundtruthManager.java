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

    /**
     * Normalize GT index base using dataset size heuristics:
     *  - If any id == 0, assume 0-based and do not convert.
     *  - If no zeros AND max == datasetSize AND min >= 1, convert 1-based → 0-based.
     *  - If -Dgt.forceOneBased=true, always convert.
     * Logs post-normalization out-of-range counts.
     */
    public synchronized void normalizeIndexBaseIfNeeded(int datasetSize) {
        if (normalized) return;
        if (datasetSize <= 0) {
            logger.warn("normalizeIndexBaseIfNeeded: datasetSize={} (skip normalization)", datasetSize);
            return;
        }

        boolean hasZero = false;
        int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE;
        for (int[] row : rows) {
            if (row == null) continue;
            for (int id : row) {
                if (id == 0) hasZero = true;
                if (id < min) min = id;
                if (id > max) max = id;
            }
        }

        boolean force1 = Boolean.getBoolean("gt.forceOneBased");
        boolean looksOneBased = (!hasZero && min >= 1 && max == datasetSize);

        if (force1 || looksOneBased) {
            for (int i = 0; i < rows.size(); i++) {
                int[] r = rows.get(i);
                if (r == null) continue;
                for (int j = 0; j < r.length; j++) r[j] = r[j] - 1;
            }
            convertedFromOneBased = true;
            logger.info("Groundtruth indices converted from 1-based to 0-based{}.",
                    force1 ? " (forced via -Dgt.forceOneBased=true)" : "");
        } else if (hasZero && min >= 0 && max <= datasetSize - 1) {
            // already 0-based; nothing to do
        } else {
            logger.warn("Groundtruth index range looks odd: min={}, max={}, datasetSize={}, hasZero={}",
                    min, max, datasetSize, hasZero);
        }

        long oor = 0L;
        for (int[] r : rows) {
            if (r == null) continue;
            for (int id : r) if (id < 0 || id >= datasetSize) oor++;
        }
        if (oor > 0) {
            logger.error("Groundtruth contains {} out-of-range ids after normalization; expected [0, {}).",
                    oor, datasetSize);
        } else {
            logger.info("Groundtruth normalized: all ids in [0, {}).", datasetSize);
        }
        normalized = true;
    }

    /** For tests/diagnostics. */
    public boolean wasConvertedFromOneBased() { return convertedFromOneBased; }

    public int[] peekFirstRow(int n) {
        if (rows.isEmpty()) return new int[0];
        int[] r = rows.get(0);
        if (r == null) return new int[0];
        return java.util.Arrays.copyOf(r, Math.min(n, r.length));
    }

    public void assertWithin(int datasetSize) {
        if (!isConsistentWithDatasetSize(datasetSize)) {
            throw new IllegalStateException("Groundtruth out of range for datasetSize=" + datasetSize);
        }
    }

}
