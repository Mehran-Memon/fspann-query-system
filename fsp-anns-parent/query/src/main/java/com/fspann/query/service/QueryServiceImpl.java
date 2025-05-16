package com.fspann.query.service;

import com.fspann.common.QueryResult;
import com.fspann.common.QueryToken;
import com.fspann.common.EncryptedPoint;
import com.fspann.crypto.CryptoService;
import com.fspann.crypto.EncryptionUtils;
import com.fspann.config.SystemConfig;
import com.fspann.index.service.IndexService;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;

import javax.crypto.SecretKey;
import java.util.Arrays;
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
        // Rotate keys if needed
        keyService.rotateIfNeeded();
        KeyVersion version = keyService.getCurrentVersion();
        SecretKey key = version.getSecretKey();

        // Decrypt the raw query vector
        byte[] raw = token.getEncryptedQuery();
        int ivLen = EncryptionUtils.generateIV()[12];
        byte[] iv = Arrays.copyOfRange(raw, 0, ivLen);
        byte[] ciphertext = Arrays.copyOfRange(raw, ivLen, raw.length);
        double[] queryVec;
        try {
            queryVec = EncryptionUtils.decryptVector(ciphertext, iv, key);
        } catch (Exception e) {
            throw new RuntimeException("Failed to decrypt query vector", e);
        }

        // Lookup encrypted candidates
        List<EncryptedPoint> candidates = indexService.lookup(token);

        // Decrypt candidates, compute distances, sort, and return top-K
        return candidates.stream()
                .map(pt -> {
                    double[] vec;
                    try {
                        vec = cryptoService.decryptFromPoint(pt, key);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to decrypt candidate: " + pt.getId(), e);
                    }
                    double dist = computeDistance(queryVec, vec);
                    return new QueryResult(pt.getId(), dist);
                })
                .sorted()
                .limit(token.getTopK())
                .collect(Collectors.toList());
    }

    /**
     * Euclidean distance between two vectors.
     */
    private double computeDistance(double[] a, double[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("Vector lengths do not match");
        }
        double sum = 0;
        for (int i = 0; i < a.length; i++) {
            double d = a[i] - b[i];
            sum += d * d;
        }
        return Math.sqrt(sum);
    }
}
