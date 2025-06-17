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
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryServiceImpl implements QueryService {

    private static final Logger logger = LoggerFactory.getLogger(QueryServiceImpl.class);
    private static final Pattern VERSION_PATTERN = Pattern.compile(".*_v(\\d+)$");

    private final IndexService indexService;
    private final CryptoService cryptoService;
    private final KeyLifeCycleService keyService;

    public QueryServiceImpl(IndexService indexService,
                            CryptoService cryptoService,
                            KeyLifeCycleService keyService) {
        this.indexService = Objects.requireNonNull(indexService);
        this.cryptoService = Objects.requireNonNull(cryptoService);
        this.keyService = Objects.requireNonNull(keyService);
    }

    @Override
    public List<QueryResult> search(QueryToken token) {
        Objects.requireNonNull(token, "QueryToken cannot be null");
        if (token.getTopK() <= 0 || token.getEncryptedQuery() == null || token.getIv() == null) {
            throw new IllegalArgumentException("Invalid or incomplete QueryToken.");
        }

        keyService.rotateIfNeeded();

        KeyVersion queryVersion = resolveKeyVersion(token.getEncryptionContext());
        SecretKey key = queryVersion.getKey();

        double[] queryVec;
        try {
            queryVec = cryptoService.decryptQuery(token.getEncryptedQuery(), token.getIv(), key);
        } catch (Exception e) {
            logger.error("❌ Failed to decrypt query vector", e);
            throw new RuntimeException("Decryption error: query vector", e);
        }

        List<EncryptedPoint> candidates = indexService.lookup(token);
        if (candidates.isEmpty()) {
            logger.warn("No candidates retrieved for the query token");
            return List.of(); // Return empty, not null
        }

        return candidates.stream()
                .map(pt -> {
                    try {
                        double[] ptVec = cryptoService.decryptFromPoint(pt, key);
                        double dist = computeDistance(queryVec, ptVec);
                        return new QueryResult(pt.getId(), dist);
                    } catch (Exception e) {
                        logger.warn("Skipped corrupt candidate point ID {} due to decryption failure", pt.getId(), e);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .sorted()
                .limit(token.getTopK())
                .collect(Collectors.toList());
    }

    private KeyVersion resolveKeyVersion(String context) {
        Matcher matcher = VERSION_PATTERN.matcher(context);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid encryption context format: " + context);
        }
        int version = Integer.parseInt(matcher.group(1));
        return keyService.getVersion(version);
    }

    private double computeDistance(double[] a, double[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("Vector dimension mismatch: " + a.length + " vs " + b.length);
        }
        double sum = 0;
        for (int i = 0; i < a.length; i++) {
            double d = a[i] - b[i];
            sum += d * d;
        }
        return Math.sqrt(sum);
    }
}
