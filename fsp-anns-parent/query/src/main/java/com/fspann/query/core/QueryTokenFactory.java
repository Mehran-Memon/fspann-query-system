package com.fspann.query.core;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;
import com.fspann.common.QueryToken;
import com.fspann.crypto.CryptoService;
import com.fspann.index.core.EvenLSH;

import javax.crypto.SecretKey;
import java.util.List;
import java.util.Objects;

public class QueryTokenFactory {
    private final CryptoService cryptoService;
    private final KeyLifeCycleService keyService;
    private final EvenLSH lsh;
    private final int numTables;

    public QueryTokenFactory(CryptoService cryptoService,
                             KeyLifeCycleService keyService,
                             EvenLSH lsh,
                             int numTables) {
        this.cryptoService = Objects.requireNonNull(cryptoService, "CryptoService must not be null");
        this.keyService    = Objects.requireNonNull(keyService, "KeyService must not be null");
        this.lsh           = Objects.requireNonNull(lsh, "EvenLSH must not be null");
        if (numTables <= 0) throw new IllegalArgumentException("numTables must be > 0");
        this.numTables = numTables;
    }

    /** Build a fresh token for a plaintext query vector and requested topK. */
    public QueryToken create(double[] vector, int topK) {
        if (vector == null || vector.length == 0) {
            throw new IllegalArgumentException("Input vector must be non-null and non-empty");
        }
        if (vector.length != lsh.getDimensions()) {
            throw new IllegalArgumentException("Vector dims mismatch: expected=" + lsh.getDimensions() + ", got=" + vector.length);
        }
        if (topK <= 0) {
            throw new IllegalArgumentException("topK must be greater than zero");
        }

        KeyVersion currentVersion = keyService.getCurrentVersion();
        SecretKey key = currentVersion.getKey();
        int version = currentVersion.getVersion();

        String encryptionContext = String.format("epoch_%d_dim_%d", version, vector.length);
        EncryptedPoint encrypted = cryptoService.encryptToPoint("query", vector, key);

        List<List<Integer>> tableBuckets = lsh.getBucketsForAllTables(vector, topK, numTables);

        return new QueryToken(
                tableBuckets,
                encrypted.getIv(),
                encrypted.getCiphertext(),
                vector.clone(),
                topK,
                numTables,
                encryptionContext,
                vector.length,
                version
        );
    }

    /** Recompute per-table expansions for a new K using the same encrypted query. */
    public QueryToken derive(QueryToken base, int newTopK) {
        Objects.requireNonNull(base, "base token");
        if (newTopK <= 0) throw new IllegalArgumentException("newTopK must be > 0");
        if (base.getQueryVector() == null || base.getQueryVector().length != lsh.getDimensions()) {
            throw new IllegalArgumentException("Base token query dims mismatch for LSH");
        }

        List<List<Integer>> tableBuckets = lsh.getBucketsForAllTables(base.getQueryVector(), newTopK, numTables);

        return new QueryToken(
                tableBuckets,
                base.getIv(),
                base.getEncryptedQuery(),
                base.getQueryVector(),
                newTopK,
                numTables,
                base.getEncryptionContext(),
                base.getDimension(),
                base.getVersion()
        );
    }
}
