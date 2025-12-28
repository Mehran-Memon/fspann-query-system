package com.fspann.common;

import java.io.Serializable;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

/**
 * Unified query token supporting:
 *  - per-table LSH bucket expansions (legacy multiprobe path)
 *  - per-table precomputed codes (BitSet per division) for partitioned indexing (mSANNP)
 *
 * NOTE: This token is encryption-only on the wire:
 *  - It carries IV + encryptedQuery bytes.
 *  - It does NOT carry the plaintext query vector.
 */
public class QueryToken implements Serializable {

    private final List<List<Integer>> tableBuckets;
    private final int numTables;

    // MSANNP integer hashes
    private final int[][] hashesByTable;

    private final byte[] iv;
    private final byte[] encryptedQuery;
    private final int topK;
    private final String encryptionContext;
    private final int dimension;
    private final int version;
    private final int lambda;

    public QueryToken(
            List<List<Integer>> tableBuckets,
            int[][] hashesByTable,
            byte[] iv,
            byte[] encryptedQuery,
            int topK,
            int numTables,
            String encryptionContext,
            int dimension,
            int version,
            int lambda
    ) {
        this.tableBuckets = List.copyOf(Objects.requireNonNull(tableBuckets));
        this.numTables = Math.max(1, numTables);
        this.hashesByTable = deepCloneHashes2D(hashesByTable);

        this.iv = iv.clone();
        this.encryptedQuery = encryptedQuery.clone();
        this.topK = Math.max(1, topK);
        this.encryptionContext = encryptionContext;
        this.dimension = dimension;
        this.version = version;
        this.lambda = lambda;
    }

    private static int[][] deepCloneHashes2D(int[][] src) {
        if (src == null) return null;
        int[][] out = new int[src.length][];
        for (int t = 0; t < src.length; t++) {
            out[t] = (src[t] == null) ? null : src[t].clone();
        }
        return out;
    }

    public List<List<Integer>> getTableBuckets() {
        return tableBuckets;
    }

    public byte[] getIv() {
        return iv.clone();
    }

    public byte[] getEncryptedQuery() {
        return encryptedQuery.clone();
    }

    public int getTopK() {
        return topK;
    }

    public int getNumTables() {
        return numTables;
    }

    public String getEncryptionContext() {
        return encryptionContext;
    }

    public int getDimension() {
        return dimension;
    }

    public int getVersion() {
        return version;
    }

    public int getLambda() {
        return lambda;
    }

    public int[][] getHashesByTable() {
        return hashesByTable;
    }
}
