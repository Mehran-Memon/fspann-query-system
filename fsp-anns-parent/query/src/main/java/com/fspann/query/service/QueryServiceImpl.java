package com.fspann.query.service;

import com.fspann.common.QueryResult;
import com.fspann.common.QueryToken;
import com.fspann.common.EncryptedPoint;
import com.fspann.crypto.CryptoService;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;
import com.fspann.index.service.IndexService;

import javax.crypto.SecretKey;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of QueryService: decrypts the encrypted query, looks up candidates,
 * decrypts each point, computes distances, sorts, and returns the top-K results.
 */
public class QueryServiceImpl implements QueryService {
    private final IndexService indexService;
    private final CryptoService cryptoService;
    private final KeyLifeCycleService keyService;
    private static final Logger logger = LoggerFactory.getLogger(QueryServiceImpl.class);

    public QueryServiceImpl(IndexService indexService,
                            CryptoService cryptoService,
                            KeyLifeCycleService keyService) {
        this.indexService = indexService;
        this.cryptoService = cryptoService;
        this.keyService = keyService;
    }

    @Override
    public List<QueryResult> search(QueryToken token) {
        // 1) Ensure key rotation
        keyService.rotateIfNeeded();

        // 2) Determine the key version from the token's context, e.g. "epoch_v7" -> 7
        String ctx = token.getEncryptionContext();
        int versionNum = Integer.parseInt(ctx.substring(ctx.lastIndexOf("_v") + 2));
        KeyVersion queryVersion = keyService.getVersion(versionNum);
        SecretKey key = queryVersion.getKey(); // Changed from getSecretKey() to getKey()

        // 3) Decrypt the query vector
        double[] queryVec;
        try {
            queryVec = cryptoService.decryptQuery(
                    token.getEncryptedQuery(),
                    token.getIv(),
                    key
            );
        } catch (Exception e) {
            logger.error("Failed to decrypt query vector", e);
            throw new RuntimeException("Failed to decrypt query vector", e);
        }

        // 4) Retrieve encrypted candidates
        List<EncryptedPoint> candidates = indexService.lookup(token);

        // 5) Decrypt, compute distance, sort, and limit to top-K
        return candidates.stream()
                .map(pt -> {
                    double[] ptVec;
                    try {
                        ptVec = cryptoService.decryptFromPoint(pt, key);
                    } catch (Exception e) {
                        logger.error("Failed to decrypt point {}", pt.getId(), e);
                        throw new RuntimeException("Failed to decrypt point " + pt.getId(), e);
                    }
                    double dist = computeDistance(queryVec, ptVec);
                    return new QueryResult(pt.getId(), dist);
                })
                .sorted()                           // sort by distance ascending (QueryResult implements Comparable)
                .limit(token.getTopK())             // take top-K
                .collect(Collectors.toList());
    }

    /**
     * Euclidean distance metric.
     */
    private double computeDistance(double[] a, double[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException(
                    "Vector dimension mismatch: " + a.length + " vs " + b.length
            );
        }
        double sum = 0;
        for (int i = 0; i < a.length; i++) {
            double d = a[i] - b[i];
            sum += d * d;
        }
        return Math.sqrt(sum);
    }
}