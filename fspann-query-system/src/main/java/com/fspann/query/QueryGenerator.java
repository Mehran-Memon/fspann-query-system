package com.fspann.query;

import com.fspann.index.EvenLSH;
import com.fspann.encryption.EncryptionUtils;
import com.fspann.keymanagement.KeyManager;
import javax.crypto.SecretKey;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Generates a QueryToken for ANN queries, mapping the query vector to buckets and encrypting it for privacy.
 */
public class QueryGenerator {

    private final EvenLSH lsh;
    private final KeyManager keyManager;

    public QueryGenerator(EvenLSH lsh, KeyManager keyManager) {
        this.lsh = lsh;
        this.keyManager = keyManager;
    }

    /**
     * Generates a QueryToken for an ANN query.
     *
     * @param queryVector The user's query vector in high-dimensional space.
     * @param topK Number of nearest neighbors requested.
     * @param expansionRange Range for neighboring buckets (e.g., 1 for ±1).
     * @return A QueryToken that can be sent to the server.
     * @throws Exception If encryption or processing fails.
     */
    public QueryToken generateQueryToken(double[] queryVector, int topK, int expansionRange) throws Exception {
        if (queryVector == null || queryVector.length == 0) {
            throw new IllegalArgumentException("Query vector cannot be null or empty.");
        }

        // 1. Compute the primary LSH bucket for the query
        int mainBucket = lsh.getBucketId(queryVector);

        // 2. Expand to neighboring buckets for better recall
        List<Integer> candidateBuckets = new ArrayList<>();
        for (int i = -expansionRange; i <= expansionRange; i++) {
            int bucket = mainBucket + i;
            if (bucket > 0 && bucket <= lsh.getCriticalValues().length + 1) { // Valid bucket range
                candidateBuckets.add(bucket);
            }
        }

        // 3. Encrypt the query vector for server privacy
        SecretKey sessionKey = keyManager.getCurrentKey();
        if (sessionKey == null) {
            throw new IllegalStateException("No current session key available.");
        }
        byte[] encryptedQuery = EncryptionUtils.encryptVector(queryVector, sessionKey);

        // 4. Build the QueryToken with the current epoch as context
        String encryptionContext = "epoch_" + keyManager.getTimeEpoch();
        return new QueryToken(candidateBuckets, encryptedQuery, topK, encryptionContext);
    }

    /**
     * Converts a double[] vector to a byte[] using ByteBuffer for efficiency.
     * @param vec The input vector.
     * @return The byte array representation.
     */
    private byte[] vectorToBytes(double[] vec) {
        ByteBuffer buffer = ByteBuffer.allocate(vec.length * Double.BYTES);
        for (double value : vec) {
            buffer.putDouble(value);
        }
        return buffer.array();
    }
}