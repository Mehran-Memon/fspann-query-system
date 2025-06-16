package com.fspann.query.core;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.QueryToken;
import com.fspann.crypto.CryptoService;
import com.fspann.crypto.EncryptionUtils; // Added for generateIV
import com.fspann.index.core.EvenLSH;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;

import javax.crypto.SecretKey;
import java.util.List;

public class QueryTokenFactory {
    private final CryptoService cryptoService;
    private final KeyLifeCycleService keyService;
    private final EvenLSH lsh;
    private final int expansionRange;
    private final int numTables;

    public QueryTokenFactory(CryptoService cryptoService, KeyLifeCycleService keyService,
                             EvenLSH lsh, int expansionRange, int numTables) {
        this.cryptoService = cryptoService;
        this.keyService = keyService;
        this.lsh = lsh;
        this.expansionRange = expansionRange;
        this.numTables = numTables;
    }

    public QueryToken create(double[] vector, int topK) {
        keyService.rotateIfNeeded();
        KeyVersion currentVersion = keyService.getCurrentVersion();
        SecretKey key = currentVersion.getKey(); // Changed from getSecretKey() to getKey()
        String encryptionContext = "epoch_v" + currentVersion.getVersion();

        byte[] iv = EncryptionUtils.generateIV();
        EncryptedPoint encrypted = cryptoService.encryptToPoint("query", vector, key);
        byte[] encryptedQuery = encrypted.getCiphertext();

        List<Integer> buckets = lsh.getBuckets(vector);
        return new QueryToken(buckets, iv, encryptedQuery, vector.clone(), topK, numTables, encryptionContext);
    }
}