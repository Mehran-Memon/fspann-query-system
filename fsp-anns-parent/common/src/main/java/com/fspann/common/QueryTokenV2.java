package com.fspann.common;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Proposal-aligned token carrying per-table expansions. */
public final class QueryTokenV2 {
    private final double[] queryVector;                 // plaintext for LSH (encryption lives in crypto path)
    private final List<List<Integer>> tableBuckets;     // per-table bucket expansions
    private final int numTables;

    // + your crypto fields: iv, encryptedQuery, epochId, version, etc.

    public QueryTokenV2(double[] queryVector, List<List<Integer>> tableBuckets) {
        this.queryVector = Objects.requireNonNull(queryVector, "queryVector");
        this.tableBuckets = Objects.requireNonNull(tableBuckets, "tableBuckets");
        this.numTables = tableBuckets.size();
        if (numTables == 0) throw new IllegalArgumentException("numTables must be > 0");
    }

    public double[] getQueryVector() { return queryVector; }
    public int getNumTables() { return numTables; }
    public List<List<Integer>> getTableBuckets() { return Collections.unmodifiableList(tableBuckets); }
}
