package java.com.fspann.query;

import java.com.fspann.index.EvenLSH;
import java.com.fspann.encryption.EncryptionUtils;
import java.com.fspann.keymanagement.KeyManager;
import javax.crypto.SecretKey;
import java.util.ArrayList;
import java.util.List;

public class QueryGenerator {
    private final EvenLSH lsh;
    private final KeyManager keyManager;

    public QueryGenerator(EvenLSH lsh, KeyManager keyManager) {
        this.lsh = lsh;
        this.keyManager = keyManager;
    }

    public QueryToken generateQueryToken(double[] queryVector, int topK, int expansionRange) throws Exception {
        if (queryVector == null || queryVector.length == 0) {
            throw new IllegalArgumentException("Query vector cannot be null or empty.");
        }

        // Get the current versioned key and generate the bucket ID
        String keyVersion = "key_v" + keyManager.getTimeVersion(); // Retrieve versioned key
        int mainBucket = lsh.getBucketId(queryVector);

        // Expand the candidate buckets range based on the query
        List<Integer> candidateBuckets = new ArrayList<>();
        for (int i = -expansionRange; i <= expansionRange; i++) {
            int bucket = mainBucket + i;
            if (bucket > 0 && bucket <= lsh.getCriticalValues().length + 1) {
                candidateBuckets.add(bucket);
            }
        }

        // Retrieve the session key based on the current key version
        SecretKey sessionKey = keyManager.getSessionKey(keyVersion);
        if (sessionKey == null) {
            throw new IllegalStateException("No session key available for version: " + keyVersion);
        }

        // Encrypt the query vector using the session key
        byte[] encryptedQuery = EncryptionUtils.encryptVector(queryVector, sessionKey);

        // Create an encryption context that includes the key version and epoch
        String encryptionContext = "version_" + keyVersion + "_epoch_" + keyManager.getTimeVersion();

        // Return the query token with candidate buckets, encrypted query vector, and other necessary information
        return new QueryToken(candidateBuckets, encryptedQuery, topK, encryptionContext);
    }
}
