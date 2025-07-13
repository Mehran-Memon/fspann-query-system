package com.fspann.query;

import com.fspann.index.EvenLSH;
import com.fspann.encryption.EncryptionUtils;
import com.fspann.keymanagement.KeyManager;

import javax.crypto.SecretKey;
import java.util.ArrayList;
import java.util.List;

public class QueryGenerator {

    private EvenLSH lsh;           // LSH instance for mapping points to buckets
    private KeyManager keyManager; // For retrieving encryption keys (if needed)

    public QueryGenerator(EvenLSH lsh, KeyManager keyManager) {
        this.lsh = lsh;
        this.keyManager = keyManager;
    }

    /**
     * Generates a QueryToken for an ANN query.
     *
     * @param queryVector The user's query vector in high-dimensional space
     * @param topK        Number of nearest neighbors requested
     * @return a QueryToken that can be sent to the server
     */
    public QueryToken generateQueryToken(double[] queryVector, int topK) {
        // 1. Compute the primary LSH bucket for the query
        int mainBucket = lsh.getBucketId(queryVector);

        // 2. (Optional) Expand to neighbors for better recall. For example, ±1 around the main bucket.
        //    This is just a simple illustration. You can adopt more advanced expansions.
        List<Integer> candidateBuckets = new ArrayList<>();
        candidateBuckets.add(mainBucket);
        candidateBuckets.add(mainBucket - 1);
        candidateBuckets.add(mainBucket + 1);

        // 3. (Optional) Encrypt the query vector for server privacy
        byte[] encryptedQuery = null;
        try {
            SecretKey sessionKey = keyManager.getSessionKey("query-session"); 
            // or generate a new one: generateSessionKey("query-session")
            if (sessionKey != null) {
                encryptedQuery = EncryptionUtils.encrypt(vectorToBytes(queryVector), sessionKey);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 4. Build the QueryToken
        return new QueryToken(candidateBuckets, encryptedQuery, topK);
    }

    private byte[] vectorToBytes(double[] vec) {
        // Convert double[] to byte[] for encryption
        // This is a simplistic approach. You could use ByteBuffer, etc.
        // Make sure you handle endianness and dimension properly.
        int length = vec.length;
        byte[] result = new byte[length * 8];
        int offset = 0;
        for (double val : vec) {
            long bits = Double.doubleToLongBits(val);
            for (int i = 0; i < 8; i++) {
                result[offset++] = (byte)((bits >> (8 * i)) & 0xFF);
            }
        }
        return result;
    }
}
