package com.fspann.query;

import com.fspann.index.EvenLSH;
import com.fspann.encryption.EncryptionUtils;
import com.fspann.keymanagement.KeyManager;
import javax.crypto.SecretKey;
import java.util.List;

public class QueryGenerator {
    private final EvenLSH lsh;
    private final KeyManager keyManager;

    // Constructor that accepts EvenLSH and KeyManager
    public QueryGenerator(EvenLSH lsh, KeyManager keyManager) {
        this.lsh = lsh;
        this.keyManager = keyManager;
    }

    // Keep the method static for generating QueryToken
    /**
     * Generates a query token from the query vector using encryption and LSH.
     * @param queryVector The query vector to process.
     * @param topK The number of nearest neighbors to retrieve.
     * @param expansionRange The range of buckets to expand for neighbors.
     * @return The generated query token.
     * @throws Exception If any error occurs during token generation.
     */
    public static QueryToken generateQueryToken(double[] queryVector, int topK, int expansionRange, EvenLSH lsh, KeyManager keyManager) throws Exception {
        // Encrypt the query vector using the current session key
        String keyVersion = "key_v" + keyManager.getTimeVersion(); // Retrieve versioned key
        SecretKey sessionKey = keyManager.getSessionKey(keyVersion);
        byte[] encryptedQuery = EncryptionUtils.encryptVector(queryVector, sessionKey);

        // Generate the main bucket ID using LSH
        int mainBucket = lsh.getBucketId(queryVector);

        // Use expandBuckets to get the expanded list of candidate buckets
        List<Integer> candidateBuckets = lsh.expandBuckets(mainBucket, expansionRange);

        // Generate the encryption context including the key version
        String encryptionContext = "version_" + keyVersion + "_epoch_" + keyManager.getTimeVersion();

        // Return the query token with candidate buckets, encrypted query vector, and other necessary information
        return new QueryToken(candidateBuckets, encryptedQuery, topK, encryptionContext);
    }}
