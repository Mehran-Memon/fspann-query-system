package com.fspann.query;

import com.fspann.index.EvenLSH;
import com.fspann.encryption.EncryptionUtils;
import com.fspann.keymanagement.KeyManager;
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

        int mainBucket = lsh.getBucketId(queryVector);
        List<Integer> candidateBuckets = new ArrayList<>();
        for (int i = -expansionRange; i <= expansionRange; i++) {
            int bucket = mainBucket + i;
            if (bucket > 0 && bucket <= lsh.getCriticalValues().length + 1) {
                candidateBuckets.add(bucket);
            }
        }

        SecretKey sessionKey = keyManager.getCurrentKey();
        if (sessionKey == null) {
            throw new IllegalStateException("No current session key available.");
        }
        byte[] encryptedQuery = EncryptionUtils.encryptVector(queryVector, sessionKey);

        String encryptionContext = "epoch_" + keyManager.getTimeEpoch();
        return new QueryToken(candidateBuckets, encryptedQuery, topK, encryptionContext);
    }
}