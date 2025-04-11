package java.com.fspann.query;

import java.com.fspann.index.EvenLSH;
import java.com.fspann.keymanagement.KeyManager;
import java.com.fspann.encryption.EncryptionUtils;
import javax.crypto.SecretKey;
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

        return EncryptionUtils.encryptVector(queryVector, sessionKey);
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

        return EncryptionUtils.decryptVector(encryptedQuery, sessionKey);
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
    public QueryToken generateQueryToken(double[] queryVector, int topK, int expansionRange, EvenLSH lsh) throws Exception {
        // Encrypt the query vector using the current session key
        byte[] encryptedQuery = encryptQuery(queryVector);

        // Generate candidate buckets based on LSH and expansion range
        String keyVersion = "key_v" + keyManager.getTimeVersion(); // Retrieve versioned key for the session
        int mainBucket = lsh.getBucketId(queryVector);

        List<Integer> candidateBuckets = lsh.expandBuckets(mainBucket, expansionRange);

        // Create the query token
        return new QueryToken(candidateBuckets, encryptedQuery, topK, "epoch_v" + keyManager.getTimeVersion());
    }
}
