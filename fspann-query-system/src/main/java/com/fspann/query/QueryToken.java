package com.fspann.query;

import java.util.List;

public class QueryToken {
    private List<Integer> candidateBuckets; // Buckets the server should search
    private byte[] encryptedQuery;          // (Optional) Encrypted query vector
    private int topK;                       // Number of neighbors requested

    public QueryToken(List<Integer> candidateBuckets, byte[] encryptedQuery, int topK) {
        this.candidateBuckets = candidateBuckets;
        this.encryptedQuery = encryptedQuery;
        this.topK = topK;
    }

    public List<Integer> getCandidateBuckets() {
        return candidateBuckets;
    }

    public byte[] getEncryptedQuery() {
        return encryptedQuery;
    }

    public int getTopK() {
        return topK;
    }
}
