package com.fspann.query.service;

import com.fspann.common.QueryResult;
import com.fspann.common.QueryToken;
import com.fspann.common.EncryptedPoint;
import com.fspann.crypto.CryptoService;
import com.fspann.key.KeyLifeCycleService;
import com.fspann.key.KeyVersion;
import com.fspann.index.service.IndexService;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Default implementation of QueryService: decrypts, ranks, and returns top-K results.
 */
public class QueryServiceImpl implements QueryService {
    private final IndexService indexService;
    private final CryptoService cryptoService;
    private final KeyLifeCycleService keyService;

    public QueryServiceImpl(IndexService indexService,
                            CryptoService cryptoService,
                            KeyLifeCycleService keyService) {
        this.indexService  = indexService;
        this.cryptoService = cryptoService;
        this.keyService    = keyService;
    }

    @Override
    public List<QueryResult> search(QueryToken token) {
        // Ensure key rotation before decrypting
        keyService.rotateIfNeeded();
        KeyVersion version = keyService.getCurrentVersion();

        // Lookup encrypted points
        List<EncryptedPoint> candidates = indexService.lookup(token);

        // Decrypt and compute distance
        return candidates.stream()
                .map(pt -> {
                    double[] vec = cryptoService.decryptFromPoint(pt, version);
                    double dist = computeDistance(token.getPlaintext(), vec);
                    return new QueryResult(pt.getId(), dist);
                })
                .sorted()
                .limit(token.getTopK())
                .collect(Collectors.toList());
    }

    // Placeholder for actual distance metric (e.g., Euclidean)
    private double computeDistance(double[] a, double[] b) {
        double sum = 0;
        for (int i = 0; i < a.length; i++) {
            double diff = a[i] - b[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }
}
