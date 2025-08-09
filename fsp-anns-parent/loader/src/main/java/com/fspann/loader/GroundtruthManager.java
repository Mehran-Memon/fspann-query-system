// loader/src/main/java/com/fspann/loader/GroundtruthManager.java
package com.fspann.loader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GroundtruthManager {
    private List<int[]> rows = new ArrayList<>();

    /** Load groundtruth from .ivecs or .csv (delegates to DefaultDataLoader). */
    public void load(String path) throws IOException {
        DefaultDataLoader ddl = new DefaultDataLoader();
        this.rows = ddl.loadGroundtruth(path);
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
        int kk = Math.min(k, full.length);
        return Arrays.copyOf(full, kk);
    }
}
