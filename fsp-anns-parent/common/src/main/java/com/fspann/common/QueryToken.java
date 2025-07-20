package com.fspann.common;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

/**
 * Data Transfer Object representing an encrypted ANN query.
 */
public class QueryToken {
    private final List<Integer> candidateBuckets;
    private final byte[] encryptedQuery;
    private final double[] plaintextQuery;
    private final byte[] iv;
    private final int topK;
    private final int numTables;
    private final String encryptionContext;
    private final int dimension;
    private final int shardId;
    private final int version;

    public QueryToken(List<Integer> candidateBuckets,
                      byte[] iv,
                      byte[] encryptedQuery,
                      double[] plaintextQuery,
                      int topK,
                      int numTables,
                      String encryptionContext,  int dimension, int shardId, int version) {
        if (candidateBuckets == null || candidateBuckets.isEmpty()) {
            throw new IllegalArgumentException("candidateBuckets cannot be null or empty");
        }
        if (encryptedQuery == null) throw new NullPointerException("encryptedQuery");
        if (plaintextQuery == null)
            throw new IllegalArgumentException("plaintextQuery cannot be null");
        if (topK < 0) {
            throw new IllegalArgumentException("topK cannot be negative: " + topK);
        }
        if (numTables <= 0) {
            throw new IllegalArgumentException("numTables must be positive: " + numTables);
        }
        this.candidateBuckets = Collections.unmodifiableList(new ArrayList<>(candidateBuckets));
        this.encryptedQuery = encryptedQuery != null ? encryptedQuery.clone() : null;
        this.plaintextQuery = plaintextQuery.clone(); // store a copy
        this.topK = topK;
        this.iv = iv.clone();
        this.numTables = numTables;
        this.encryptionContext = (encryptionContext != null && !encryptionContext.isEmpty())
                ? encryptionContext : "epoch_0";
        this.dimension = dimension;
        this.shardId = shardId;
        this.version = version;
    }

    public List<Integer> getBuckets() {
        return candidateBuckets;
    }

    public List<Integer> getCandidateBuckets() {
        return new ArrayList<>(candidateBuckets); // Return copy
    }

    public byte[] getIv() {
        return iv.clone();
    }

    public byte[] getEncryptedQuery() {
        return encryptedQuery.clone();
    }

    public double[] getPlaintextQuery() {
        return plaintextQuery.clone();
    }

    public double[] getQueryVector() {
        return getPlaintextQuery();
    }

    public int getQueryVectorLength() {
        return plaintextQuery != null ? plaintextQuery.length : 0;
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

    public int getShardId() {
        return shardId;
    }

    public int getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return "QueryToken{" +
                "candidateBuckets=" + candidateBuckets +
                ", topK=" + topK +
                ", numTables=" + numTables +
                ", encryptionContext='" + encryptionContext + '\'' +
                '}';
    }
}
