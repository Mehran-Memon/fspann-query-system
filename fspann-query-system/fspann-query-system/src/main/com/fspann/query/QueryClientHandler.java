package com.fspann.query;

import com.fspann.index.EvenLSH;
import com.fspann.keymanagement.KeyManager;
import com.fspann.encryption.EncryptionUtils;
import javax.crypto.SecretKey;
import java.util.Arrays;
import java.util.List;

public class QueryClientHandler {
    private final KeyManager keyManager;

    // Constructor to initialize the KeyManager
    public QueryClientHandler(KeyManager keyManager) {
        this.keyManager = keyManager;
    }

    /**
     * Encrypts a query vector using the current session key.
     * @param queryVector The query vector to encrypt.
     * @return The encrypted query vector.
     * @throws Exception If encryption fails.
     */
    public byte[] encryptQuery(double[] queryVector) throws Exception {
        SecretKey sessionKey = keyManager.getCurrentKey();
        if (sessionKey == null) {
            throw new IllegalStateException("No session key available.");
        }

        byte[] iv = EncryptionUtils.generateIV();  // Generate IV for encryption
        return EncryptionUtils.encryptVector(queryVector, iv, sessionKey);  // Pass IV to encryptVector
    }


    /**
     * Decrypts the encrypted query vector using the current key.
     * @param encryptedQuery The encrypted query vector.
     * @return The decrypted query vector.
     * @throws Exception If decryption fails.
     */
    public double[] decryptQuery(byte[] encryptedQuery) throws Exception {
        SecretKey sessionKey = keyManager.getCurrentKey();
        if (sessionKey == null) {
            throw new IllegalStateException("No session key available.");
        }

        // The IV is usually stored in the first 16 bytes of the encrypted data
        byte[] iv = Arrays.copyOfRange(encryptedQuery, 0, 16);  // Extract the IV from the encrypted data
        return EncryptionUtils.decryptVector(encryptedQuery, iv, sessionKey);  // Pass IV to decryptVector
    }


    /**
     * Generate a query token for the provided query vector, used for querying the index.
     * @param queryVector The query vector.
     * @param topK The number of nearest neighbors to retrieve.
     * @param expansionRange The range to expand for candidate buckets.
     * @param lsh The LSH function used for generating the bucket ID.
     * @return The generated query token.
     * @throws Exception If any error occurs during query token generation.
     */
    public QueryToken generateQueryToken(double[] queryVector, int topK, int expansionRange, int numTables, EvenLSH lsh) throws Exception {
        byte[] encryptedQuery = encryptQuery(queryVector);
        String keyVersion = "key_v" + keyManager.getTimeVersion();
        int mainBucket = lsh.getBucketId(queryVector);
        List<Integer> candidateBuckets = lsh.expandBuckets(mainBucket, expansionRange);
        return new QueryToken(candidateBuckets, encryptedQuery, topK, numTables, "epoch_v" + keyManager.getTimeVersion());
    }
}