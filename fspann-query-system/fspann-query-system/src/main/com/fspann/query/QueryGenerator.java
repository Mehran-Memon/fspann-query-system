package com.fspann.query;

import com.fspann.index.EvenLSH;
import com.fspann.keymanagement.KeyManager;
import com.fspann.encryption.EncryptionUtils;
import java.util.ArrayList;
import java.util.List;

public class QueryGenerator {
    private final EvenLSH lsh;
    private final KeyManager keyManager;

    // Constructor that accepts EvenLSH and KeyManager
    public QueryGenerator(EvenLSH lsh, KeyManager keyManager) {
        this.lsh = lsh;
        this.keyManager = keyManager;
    }

    /**
     * Generates a query token from the query vector using encryption and LSH.
     *
     * @param queryVector The query vector to process.
     * @param k           The number of nearest neighbors to retrieve.
     * @param numTables   The number of hash tables to query.
     * @param lsh         The LSH function to compute bucket IDs.
     * @param keyManager  The key manager for encryption.
     * @return The generated query token.
     * @throws Exception If any error occurs during token generation.
     */
    public static QueryToken generateQueryToken(double[] queryVector, int k, int numTables, EvenLSH lsh, KeyManager keyManager) throws Exception {
        List<Integer> candidateBuckets = new ArrayList<>();
        int bucketId = lsh.getBucketId(queryVector);
        candidateBuckets.add(bucketId);
        byte[] encryptedQuery = EncryptionUtils.encryptVector(queryVector, keyManager.getCurrentKey());
        String encryptionContext = "epoch_0";
        return new QueryToken(candidateBuckets, encryptedQuery, k, numTables, encryptionContext);
    }
}