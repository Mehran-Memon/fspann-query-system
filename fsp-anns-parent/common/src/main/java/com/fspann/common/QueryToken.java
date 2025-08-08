package com.fspann.common;

import java.util.List;
import java.util.Objects;

public final class QueryToken {
    private final List<List<Integer>> tableBuckets; // per-table contiguous expansions
    private final byte[] iv;
    private final byte[] encryptedQuery;
    private final double[] queryVector; // plaintext copy for eval
    private final int topK;
    private final int numTables;
    private final String encryptionContext; // "epoch_<v>_dim_<d>"
    private final int dimension;
    private final int version;

    public QueryToken(List<List<Integer>> tableBuckets,
                      byte[] iv,
                      byte[] encryptedQuery,
                      double[] queryVector,
                      int topK,
                      int numTables,
                      String encryptionContext,
                      int dimension,
                      int version) {
        this.tableBuckets      = Objects.requireNonNull(tableBuckets, "tableBuckets");
        this.iv                = Objects.requireNonNull(iv, "iv");
        this.encryptedQuery    = Objects.requireNonNull(encryptedQuery, "encryptedQuery");
        this.queryVector       = Objects.requireNonNull(queryVector, "queryVector");
        this.encryptionContext = Objects.requireNonNull(encryptionContext, "encryptionContext");
        if (topK <= 0) throw new IllegalArgumentException("topK must be > 0");
        if (numTables <= 0) throw new IllegalArgumentException("numTables must be > 0");
        if (tableBuckets.size() != numTables)
            throw new IllegalArgumentException("tableBuckets size must equal numTables");
        this.topK = topK;
        this.numTables = numTables;
        this.dimension = dimension;
        this.version = version;
    }

    public List<List<Integer>> getTableBuckets() { return tableBuckets; }
    public byte[] getIv() { return iv; }
    public byte[] getEncryptedQuery() { return encryptedQuery; }
    public double[] getQueryVector() { return queryVector; }
    public int getTopK() { return topK; }
    public int getNumTables() { return numTables; }
    public String getEncryptionContext() { return encryptionContext; }
    public int getDimension() { return dimension; }
    public int getVersion() { return version; }
}
