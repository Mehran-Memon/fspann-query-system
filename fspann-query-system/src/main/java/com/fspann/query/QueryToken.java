package com.fspann.query;

import java.util.Collections;
import java.util.List;

/**
 * Represents a query token for ANN queries, encapsulating candidate buckets, an encrypted query vector,
 * and the number of nearest neighbors requested. Supports forward security with an optional encryption context.
 */
public class QueryToken {

    private final List<Integer> candidateBuckets; // Buckets the server should search
    private final byte[] encryptedQuery;          // (Optional) Encrypted query vector
    private final int topK;                       // Number of neighbors requested
    private final String encryptionContext;       // Optional context (e.g., "epoch_<number>") for key rotation

    /**
     * Constructs a QueryToken.
     * @param candidateBuckets List of bucket IDs to search.
     * @param encryptedQuery Encrypted query vector (can be null if not encrypted).
     * @param topK Number of nearest neighbors requested.
     * @param encryptionContext Context for the encryption key (e.g., "epoch_0"), or null if not applicable.
     * @throws IllegalArgumentException If candidateBuckets is null or topK is negative.
     */
    public QueryToken(List<Integer> candidateBuckets, byte[] encryptedQuery, int topK, String encryptionContext) {
        if (candidateBuckets == null) {
            throw new IllegalArgumentException("candidateBuckets cannot be null");
        }
        if (topK < 0) {
            throw new IllegalArgumentException("topK cannot be negative: " + topK);
        }
        this.candidateBuckets = Collections.unmodifiableList(new ArrayList<>(candidateBuckets)); // Defensive copy
        this.encryptedQuery = encryptedQuery != null ? encryptedQuery.clone() : null; // Defensive copy
        this.topK = topK;
        this.encryptionContext = encryptionContext != null ? encryptionContext : "epoch_0"; // Default context
    }

    /**
     * Constructs a QueryToken without an encryption context (uses default).
     * @param candidateBuckets List of bucket IDs to search.
     * @param encryptedQuery Encrypted query vector (can be null).
     * @param topK Number of nearest neighbors requested.
     * @throws IllegalArgumentException If candidateBuckets is null or topK is negative.
     */
    public QueryToken(List<Integer> candidateBuckets, byte[] encryptedQuery, int topK) {
        this(candidateBuckets, encryptedQuery, topK, null);
    }

    /**
     * Gets the candidate buckets (immutable view).
     * @return List of bucket IDs.
     */
    public List<Integer> getCandidateBuckets() {
        return candidateBuckets;
    }

    /**
     * Gets the encrypted query vector.
     * @return The encrypted query byte array, or null if not encrypted.
     */
    public byte[] getEncryptedQuery() {
        return encryptedQuery != null ? encryptedQuery.clone() : null; // Defensive copy
    }

    /**
     * Gets the number of nearest neighbors requested.
     * @return The topK value.
     */
    public int getTopK() {
        return topK;
    }

    /**
     * Gets the encryption context (e.g., epoch or key ID).
     * @return The encryption context string.
     */
    public String getEncryptionContext() {
        return encryptionContext;
    }
}