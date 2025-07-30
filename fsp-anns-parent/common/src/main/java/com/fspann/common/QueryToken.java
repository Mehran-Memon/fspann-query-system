package com.fspann.common;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Objects;

/**
 * Data Transfer Object representing an encrypted ANN query.
 * Immutable once constructed.
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
                      String encryptionContext,
                      int dimension,
                      int shardId,
                      int version) {

        Objects.requireNonNull(candidateBuckets, "candidateBuckets cannot be null");
        Objects.requireNonNull(encryptedQuery, "encryptedQuery cannot be null");
        Objects.requireNonNull(plaintextQuery, "plaintextQuery cannot be null");
        Objects.requireNonNull(iv, "IV cannot be null");

        if (candidateBuckets.isEmpty()) {
            throw new IllegalArgumentException("candidateBuckets cannot be empty");
        }
        if (topK <= 0) {
            throw new IllegalArgumentException("topK must be positive: " + topK);
        }
        if (numTables <= 0) {
            throw new IllegalArgumentException("numTables must be positive: " + numTables);
        }
        if (dimension <= 0) {
            throw new IllegalArgumentException("dimension must be positive: " + dimension);
        }

        this.candidateBuckets = Collections.unmodifiableList(new ArrayList<>(candidateBuckets));
        this.iv = iv.clone();
        this.encryptedQuery = encryptedQuery.clone();
        this.plaintextQuery = plaintextQuery.clone();
        this.topK = topK;
        this.numTables = numTables;
        this.encryptionContext = (encryptionContext != null && !encryptionContext.isEmpty())
                ? encryptionContext
                : "epoch_0_dim_" + dimension;
        this.dimension = dimension;
        this.shardId = shardId;
        this.version = version;
    }

    public List<Integer> getBuckets() {
        return candidateBuckets;
    }

    public List<Integer> getCandidateBuckets() {
        return new ArrayList<>(candidateBuckets);
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
        return plaintextQuery.length;
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
                "topK=" + topK +
                ", numTables=" + numTables +
                ", buckets=" + candidateBuckets +
                ", encryptionContext='" + encryptionContext + '\'' +
                ", version=" + version +
                ", dim=" + dimension +
                ", shardId=" + shardId +
                '}';
    }
}
