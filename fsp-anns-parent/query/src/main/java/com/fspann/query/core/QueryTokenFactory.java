package com.fspann.query.core;

import com.fspann.common.QueryToken;
import com.fspann.common.EncryptedPoint;
import com.fspann.crypto.CryptoService;
import com.fspann.key.KeyLifeCycleService;
import com.fspann.key.KeyVersion;
import com.fspann.index.core.EvenLSH;

import javax.crypto.SecretKey;
import java.util.List;

/**
 * Factory for generating QueryToken from raw query vectors.
 */
public class QueryTokenFactory {
    private final CryptoService cryptoService;
    private final KeyLifeCycleService keyService;
    private final EvenLSH lsh;
    private final int expansionRange;
    private final int numTables;

    public QueryTokenFactory(CryptoService cryptoService,
                             KeyLifeCycleService keyService,
                             EvenLSH lsh,
                             int expansionRange,
                             int numTables) {
        this.cryptoService  = cryptoService;
        this.keyService     = keyService;
        this.lsh            = lsh;
        this.expansionRange = expansionRange;
        this.numTables      = numTables;
    }

    /**
     * Create a QueryToken by encrypting the query vector and computing bucket candidates.
     *
     * @param queryVector raw feature vector
     * @param topK number of neighbors to retrieve
     * @return QueryToken for secure ANN lookup
     */
    public QueryToken create(double[] queryVector, int topK) {
        // Rotate key if needed
        keyService.rotateIfNeeded();

        // Fetch current key version and extract SecretKey
        KeyVersion version = keyService.getCurrentVersion();
        SecretKey key = version.getSecretKey();

        // Encrypt the query vector
        EncryptedPoint ep = cryptoService.encryptToPoint("query", queryVector, key);

        // Determine primary bucket and its neighbors
        int mainBucket = lsh.getBucketId(queryVector);
        List<Integer> buckets = lsh.expandBuckets(mainBucket, expansionRange);

        // Build and return token (ciphertext includes IV)
        return new QueryToken(
                buckets,
                ep.getCiphertext(),
                topK,
                numTables,
                "epoch_v" + version.getVersion()
        );
    }
}