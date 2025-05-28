package com.fspann.query.core;

import com.fspann.common.QueryToken;
import com.fspann.common.EncryptedPoint;
import com.fspann.crypto.CryptoService;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;
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
        keyService.rotateIfNeeded();
        KeyVersion ver = keyService.getCurrentVersion();

        EncryptedPoint ep = cryptoService.encryptToPoint(
                "query", queryVector, ver.getSecretKey()
        );

        int mainBucket = lsh.getBucketId(queryVector);
        List<Integer> buckets = lsh.expandBuckets(mainBucket, expansionRange);

        return new QueryToken(
                buckets,
                ep.getIv(),
                ep.getCiphertext(),
                topK,
                numTables,
                "epoch_v" + ver.getVersion()
        );
    }
}