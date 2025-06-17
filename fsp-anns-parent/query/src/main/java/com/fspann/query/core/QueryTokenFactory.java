package com.fspann.query.core;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.QueryToken;
import com.fspann.crypto.CryptoService;
import com.fspann.crypto.EncryptionUtils;
import com.fspann.index.core.EvenLSH;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;

import javax.crypto.SecretKey;
import java.util.List;
import java.util.Objects;

public class QueryTokenFactory {
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

        keyService.rotateIfNeeded();
        KeyVersion currentVersion = keyService.getCurrentVersion();
        SecretKey key = currentVersion.getKey();
        String encryptionContext = "epoch_v" + currentVersion.getVersion();

        byte[] iv = EncryptionUtils.generateIV();
        EncryptedPoint encrypted = cryptoService.encryptToPoint("query", vector, key);
        byte[] encryptedQuery = encrypted.getCiphertext();

        // Using the getBuckets method of EvenLSH
        List<Integer> buckets = lsh.getBuckets(vector);  // This will now work as expected

        return new QueryToken(buckets, iv, encryptedQuery, vector.clone(), topK, numTables, encryptionContext);
    }
}