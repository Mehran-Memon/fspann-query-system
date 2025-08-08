package com.fspann.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Unified query token supporting both legacy single-bucket set and per-table expansions.
 * Prefer the per-table constructor going forward.
 */
public class QueryToken implements Serializable {
    private static final long serialVersionUID = 1L;

    // --- new preferred fields ---
    private final List<List<Integer>> tableBuckets;  // per-table expansions
    private final int numTables;

    // --- legacy fields (kept for compatibility) ---
    @Deprecated
    private final List<Integer> candidateBuckets;
    @Deprecated
    private final int shardId; // ignored by new path

    // --- shared fields ---
    private final byte[] iv;
    private final byte[] encryptedQuery;
    private final double[] queryVector;
    private final int topK;
    private final String encryptionContext;
    private final int dimension;
    private final int version;

    // ---- Preferred constructor (per-table) ----
    public QueryToken(List<List<Integer>> tableBuckets,
                      byte[] iv,
                      byte[] encryptedQuery,
                      double[] queryVector,
                      int topK,
                      int numTables,
                      String encryptionContext,
                      int dimension,
                      int version) {
        this.tableBuckets = deepUnmodifiableBuckets(Objects.requireNonNull(tableBuckets, "tableBuckets"));
        this.numTables = positive(numTables, "numTables");
        this.iv = Objects.requireNonNull(iv, "iv");
        this.encryptedQuery = Objects.requireNonNull(encryptedQuery, "encryptedQuery");
        this.queryVector = Objects.requireNonNull(queryVector, "queryVector").clone();
        this.topK = positive(topK, "topK");
        this.encryptionContext = Objects.requireNonNull(encryptionContext, "encryptionContext");
        this.dimension = positive(dimension, "dimension");
        this.version = version;

        // legacy
        this.candidateBuckets = List.of();
        this.shardId = 0;

        if (this.tableBuckets.size() != this.numTables) {
            throw new IllegalArgumentException("tableBuckets.size() must equal numTables");
        }
    }

    // ---- Legacy constructor (kept so old tests & callers continue to compile) ----
    @Deprecated
    public QueryToken(List<Integer> candidateBuckets,
                      byte[] iv,
                      byte[] encryptedQuery,
                      double[] plaintextQuery,
                      int topK,
                      int numTables,
                      String encryptionContext,
                      int dimension,
                      int shardId,
                      int version) {
        this.candidateBuckets = List.copyOf(Objects.requireNonNull(candidateBuckets, "candidateBuckets"));
        this.iv = Objects.requireNonNull(iv, "iv");
        this.encryptedQuery = Objects.requireNonNull(encryptedQuery, "encryptedQuery");
        this.queryVector = Objects.requireNonNull(plaintextQuery, "plaintextQuery").clone();
        this.topK = positive(topK, "topK");
        this.numTables = positive(numTables, "numTables");
        this.encryptionContext = (encryptionContext == null || encryptionContext.isBlank())
                ? ("epoch_" + version + "_dim_" + dimension) : encryptionContext;
        this.dimension = positive(dimension, "dimension");
        this.shardId = shardId;
        this.version = version;

        // new path absent
        this.tableBuckets = null;
    }

    // ---- helpers ----
    public boolean hasPerTable() { return tableBuckets != null && !tableBuckets.isEmpty(); }

    public List<List<Integer>> getTableBuckets() {
        return tableBuckets == null ? List.of() : tableBuckets;
    }

    /** If legacy token, replicate candidateBuckets across tables. */
    public List<List<Integer>> getTableBucketsOrLegacy(int tables) {
        if (hasPerTable()) return tableBuckets;
        int t = (tables <= 0) ? this.numTables : tables;
        List<List<Integer>> out = new ArrayList<>(t);
        for (int i = 0; i < t; i++) out.add(candidateBuckets);
        return out;
    }

    private static List<List<Integer>> deepUnmodifiableBuckets(List<List<Integer>> src) {
        List<List<Integer>> out = new ArrayList<>(src.size());
        for (List<Integer> l : src) out.add(List.copyOf(l));
        return Collections.unmodifiableList(out);
    }

    private static int positive(int v, String name) {
        if (v <= 0) throw new IllegalArgumentException(name + " must be > 0");
        return v;
    }

    // ---- getters (legacy names preserved) ----
    @Deprecated
    public List<Integer> getCandidateBuckets() { return candidateBuckets; }

    public List<List<Integer>> getPerTableBuckets() { return getTableBuckets(); }

    public byte[] getIv() { return iv; }

    public byte[] getEncryptedQuery() { return encryptedQuery; }

    public double[] getPlaintextQuery() { return queryVector.clone(); }

    // alias for some callers
    public double[] getQueryVector() { return getPlaintextQuery(); }

    public int getTopK() { return topK; }

    public int getNumTables() { return numTables; }

    public String getEncryptionContext() { return encryptionContext; }

    public int getDimension() { return dimension; }

    public int getVersion() { return version; }

    @Deprecated
    public int getShardId() { return shardId; }
}
