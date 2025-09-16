package com.fspann.query.core;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;
import com.fspann.common.QueryToken;
import com.fspann.crypto.CryptoService;
import com.fspann.index.core.EvenLSH;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class QueryTokenFactory {
    private static final Logger logger = LoggerFactory.getLogger(QueryTokenFactory.class);

    private final CryptoService cryptoService;
    private final KeyLifeCycleService keyService;
    private final EvenLSH lsh;
    /** Unused with contiguous/Hamming expansion. Retained for compatibility. */
    @SuppressWarnings("unused")
    private final int expansionRange;
    private final int numTables;

    public QueryTokenFactory(CryptoService cryptoService,
                             KeyLifeCycleService keyService,
                             EvenLSH lsh,
                             int expansionRange,
                             int numTables) {
        this.cryptoService = Objects.requireNonNull(cryptoService, "CryptoService must not be null");
        this.keyService = Objects.requireNonNull(keyService, "KeyService must not be null");
        this.lsh = Objects.requireNonNull(lsh, "EvenLSH must not be null");
        if (numTables <= 0) throw new IllegalArgumentException("numTables must be positive");
        if (expansionRange < 0) throw new IllegalArgumentException("expansionRange must be >= 0");
        this.expansionRange = expansionRange;
        this.numTables = numTables;
    }

    public QueryTokenFactory(CryptoService cryptoService,
                             KeyLifeCycleService keyService,
                             EvenLSH lsh,
                             int numTables) {
        this(cryptoService, keyService, lsh, 0, numTables);
    }

    public QueryToken create(double[] vector, int topK) {
        Objects.requireNonNull(vector, "vector");
        if (vector.length == 0) throw new IllegalArgumentException("vector must be non-empty");
        if (topK <= 0) throw new IllegalArgumentException("topK must be > 0");
        int lshDim = lsh.getDimensions();
        if (lshDim > 0 && lshDim != vector.length) {
            throw new IllegalArgumentException("Vector dimension mismatch: expected " + lshDim + " but got " + vector.length);
        }

        KeyVersion currentVersion = keyService.getCurrentVersion();
        SecretKey key = currentVersion.getKey();
        int version = currentVersion.getVersion();
        String encryptionContext = "epoch_" + version + "_dim_" + vector.length;

        EncryptedPoint ep = cryptoService.encryptToPoint("query", vector, key);

        List<List<Integer>> perTable = lsh.getBucketsForAllTables(vector, topK, numTables);
        // deep mutable copy
        List<List<Integer>> copy = new ArrayList<>(numTables);
        if (perTable == null || perTable.size() != numTables) {
            for (int t = 0; t < numTables; t++) {
                copy.add(new ArrayList<>(lsh.getBuckets(vector, topK, t)));
            }
        } else {
            for (List<Integer> l : perTable) copy.add(new ArrayList<>(l));
        }

        int total = copy.stream().mapToInt(List::size).sum();
        logger.debug("Created token: dim={}, topK={}, tables={}, totalProbes={}", vector.length, topK, numTables, total);

        return new QueryToken(
                copy,
                ep.getIv(),
                ep.getCiphertext(),
                vector.clone(),
                topK,
                numTables,
                encryptionContext,
                vector.length,
                version
        );
    }

    public QueryToken derive(QueryToken base, int newTopK) {
        Objects.requireNonNull(base, "base");
        if (newTopK <= 0) throw new IllegalArgumentException("newTopK must be > 0");

        double[] q = base.getPlaintextQuery();
        int lshDim = lsh.getDimensions();
        if (lshDim > 0 && lshDim != q.length) {
            throw new IllegalArgumentException("Vector dimension mismatch: expected " + lshDim + " but got " + q.length);
        }

        // Use the SAME version as the base token
        int version = base.getVersion();
        KeyVersion kv = keyService.getVersion(version);
        EncryptedPoint ep = cryptoService.encryptToPoint("query", q, kv.getKey());

        List<List<Integer>> perTable = lsh.getBucketsForAllTables(q, newTopK, numTables);
        List<List<Integer>> copy = new ArrayList<>(numTables);
        if (perTable == null || perTable.size() != numTables) {
            for (int t = 0; t < numTables; t++) copy.add(new ArrayList<>(lsh.getBuckets(q, newTopK, t)));
        } else {
            for (List<Integer> l : perTable) copy.add(new ArrayList<>(l));
        }

        logger.debug("Derived token: dim={}, topK={}, tables={}, totalProbes={}",
                q.length, newTopK, numTables, copy.stream().mapToInt(List::size).sum());

        return new QueryToken(
                copy,
                ep.getIv(),
                ep.getCiphertext(),
                q.clone(),
                newTopK,
                numTables,
                base.getEncryptionContext(), // preserve context
                q.length,
                version                      // preserve version
        );
    }
}