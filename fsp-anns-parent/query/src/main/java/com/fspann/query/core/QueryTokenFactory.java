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
    private final int expansionRange; // kept for compatibility (unused with contiguous logic)
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

    public QueryTokenFactory(CryptoService cryptoService,
                             KeyLifeCycleService keyService,
                             EvenLSH lsh,
                             int numTables) {
        this(cryptoService, keyService, lsh, /*expansionRange=*/0, numTables);
    }

    /** Build a fresh per-table QueryToken for a topK. */
    public QueryToken create(double[] vector, int topK) {
        if (vector == null || vector.length == 0) {
            throw new IllegalArgumentException("Input vector must be non-null and non-empty");
        }
        if (topK <= 0) throw new IllegalArgumentException("topK must be greater than zero");

        // NEW: dimension validation against LSH
        int lshDim = lsh.getDimensions();
        if (lshDim > 0 && lshDim != vector.length) {
            throw new IllegalArgumentException(
                    "Vector dimension mismatch: expected " + lshDim + " but got " + vector.length);
        }

        KeyVersion currentVersion = keyService.getCurrentVersion();
        SecretKey key = currentVersion.getKey();
        int version = currentVersion.getVersion();

        String encryptionContext = String.format("epoch_%d_dim_%d", version, vector.length);

        // FIX: use "query" to match tests
        EncryptedPoint encrypted = cryptoService.encryptToPoint("query", vector, key);

        List<List<Integer>> perTable = new ArrayList<>(numTables);
        for (int t = 0; t < numTables; t++) {
            perTable.add(lsh.getBuckets(vector, topK, t));
        }

        logger.debug("Created per-table QueryToken: version={}, dimension={}, tables={}", version, vector.length, numTables);
        return new QueryToken(
                perTable,
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

    /** Derive the same token but with a different K (recomputes per-table expansions). */
    public QueryToken derive(QueryToken base, int newTopK) {
        Objects.requireNonNull(base, "base");
        if (newTopK <= 0) throw new IllegalArgumentException("newTopK must be > 0");

        double[] q = base.getPlaintextQuery();

        // Optional: keep the same validation in derive as well
        int lshDim = lsh.getDimensions();
        if (lshDim > 0 && lshDim != q.length) {
            throw new IllegalArgumentException(
                    "Vector dimension mismatch: expected " + lshDim + " but got " + q.length);
        }

        KeyVersion curr = keyService.getCurrentVersion();

        // FIX: use "query" here too
        EncryptedPoint ep = cryptoService.encryptToPoint("query", q, curr.getKey());

        List<List<Integer>> perTable = new ArrayList<>(numTables);
        for (int t = 0; t < numTables; t++) {
            perTable.add(lsh.getBuckets(q, newTopK, t));
        }

        return new QueryToken(
                perTable,
                ep.getIv(),
                ep.getCiphertext(),
                q,
                newTopK,
                numTables,
                String.format("epoch_%d_dim_%d", curr.getVersion(), q.length),
                q.length,
                curr.getVersion()
        );
    }
}