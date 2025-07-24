package com.fspann.query.core;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.QueryToken;
import com.fspann.crypto.CryptoService;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;
import com.fspann.index.core.EvenLSH;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.util.List;
import java.util.Objects;

public class QueryTokenFactory {
    private static final Logger logger = LoggerFactory.getLogger(QueryTokenFactory.class);
    private final CryptoService cryptoService;
    private final KeyLifeCycleService keyService;
    private final EvenLSH lsh;
    private final int expansionRange;
    private final int numTables;

    public QueryTokenFactory(CryptoService cryptoService, KeyLifeCycleService keyService,
                             EvenLSH lsh, int expansionRange, int numTables) {
        this.cryptoService = Objects.requireNonNull(cryptoService, "CryptoService must not be null");
        this.keyService = Objects.requireNonNull(keyService, "KeyService must not be null");
        this.lsh = Objects.requireNonNull(lsh, "EvenLSH must not be null");
        if (expansionRange < 0 || numTables <= 0) {
            throw new IllegalArgumentException("Expansion range and number of tables must be positive");
        }
        this.expansionRange = expansionRange;
        this.numTables = numTables;
    }

    public QueryToken create(double[] vector, int topK) {
        if (vector == null || vector.length == 0) {
            throw new IllegalArgumentException("Input vector must be non-null and non-empty");
        }
        if (topK <= 0) {
            throw new IllegalArgumentException("topK must be greater than zero");
        }

        KeyVersion currentVersion = keyService.getCurrentVersion();
        SecretKey key = currentVersion.getKey();
        int version = currentVersion.getVersion();

        String encryptionContext = String.format("epoch_%d_dim_%d", version, vector.length);
        EncryptedPoint encrypted = cryptoService.encryptToPoint("index", vector, key);
        byte[] iv = encrypted.getIv();
        byte[] encryptedQuery = encrypted.getCiphertext();

        List<Integer> buckets = lsh.getBuckets(vector);
        int shardId = buckets.isEmpty() ? 0 : Math.abs(buckets.hashCode() % numTables);

        logger.debug("Created QueryToken: version={}, dimension={}, shardId={}", version, vector.length, shardId);

        return new QueryToken(
                buckets,
                iv,
                encryptedQuery,
                vector.clone(),
                topK,
                numTables,
                encryptionContext,
                vector.length,
                shardId,
                version
        );
    }
}