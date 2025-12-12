package com.fspann.query.core;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;
import com.fspann.common.QueryToken;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.CryptoService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.util.*;

/**
 * QueryTokenFactory (LSH-ONLY VERSION)
 * ====================================
 *
 * Pure LSH token generation - no PAPER codes.
 *
 * Responsibilities:
 *   • Encrypt query vector
 *   • Compute LSH bucket hints (if available)
 *   • Create QueryToken for LSH search
 *
 * Key simplifications:
 *   ✅ No PAPER partition codes
 *   ✅ No divisions, m, lambda, seed parameters
 *   ✅ Single path - pure LSH
 *   ✅ Clean, minimal implementation
 *
 * @author FSP-ANNS Project
 * @version 4.0 (LSH-Only)
 */
public final class QueryTokenFactory {

    private static final Logger log = LoggerFactory.getLogger(QueryTokenFactory.class);

    private final CryptoService crypto;
    private final KeyLifeCycleService keyService;
    private final int numTables;
    private final int probeRange;
    private final SystemConfig cfg;

    // ────────────────────────────────────────────────────────────────────

    /**
     * Constructor for LSH-only query token factory.
     *
     * @param crypto crypto service
     * @param keyService key lifecycle service
     * @param numTables number of LSH tables
     * @param probeRange probe range hint
     * @param cfg system configuration
     */
    public QueryTokenFactory(
            CryptoService crypto,
            KeyLifeCycleService keyService,
            int numTables,
            int probeRange,
            SystemConfig cfg) {

        this.crypto = Objects.requireNonNull(crypto, "crypto");
        this.keyService = Objects.requireNonNull(keyService, "keyService");
        this.numTables = Math.max(0, numTables);
        this.probeRange = Math.max(0, probeRange);
        this.cfg = Objects.requireNonNull(cfg, "cfg");

        log.info("QueryTokenFactory (LSH-Only): tables={}, probeRange={}",
                numTables, probeRange);
    }

    // ────────────────────────────────────────────────────────────────────
    // PUBLIC API
    // ────────────────────────────────────────────────────────────────────

    /**
     * Create query token for LSH search.
     *
     * Pure LSH path - no PAPER codes.
     */
    public QueryToken create(double[] vec, int topK) {
        Objects.requireNonNull(vec, "query vector");
        if (topK <= 0) throw new IllegalArgumentException("topK must be > 0");

        if (vec.length == 0) {
            throw new IllegalArgumentException("Invalid query vector");
        }

        final int dim = vec.length;

        /* 1) Empty bucket lists (LSH not actively used in token) */
        List<List<Integer>> buckets = emptyTables(numTables);

        /* 2) Encrypt query */
        KeyVersion kv = keyService.getCurrentVersion();
        SecretKey sk = kv.getKey();
        EncryptedPoint ep = crypto.encryptToPoint("query", vec, sk);

        /* 3) Empty codes (no PAPER in LSH-only) */
        BitSet[] codes = emptyBitSets();

        /* 4) Token construction */
        return new QueryToken(
                buckets,
                codes,
                ep.getIv(),
                ep.getCiphertext(),
                topK,
                numTables,
                "dim_" + dim + "_v" + kv.getVersion(),
                dim,
                kv.getVersion()
        );
    }

    // ────────────────────────────────────────────────────────────────────
    // HELPERS
    // ────────────────────────────────────────────────────────────────────

    private List<List<Integer>> emptyTables(int n) {
        List<List<Integer>> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            out.add(Collections.emptyList());
        }
        return out;
    }

    private BitSet[] emptyBitSets() {
        // No divisions in LSH-only, but return minimal array for compatibility
        BitSet[] out = new BitSet[1];
        out[0] = new BitSet(0);  // Empty bitset
        return out;
    }

    // ────────────────────────────────────────────────────────────────────

    public QueryToken derive(QueryToken base, int newTopK) {
        return new QueryToken(
                base.getTableBuckets(),
                base.getCodes(),
                base.getIv(),
                base.getEncryptedQuery(),
                newTopK,
                base.getNumTables(),
                base.getEncryptionContext(),
                base.getDimension(),
                base.getVersion()
        );
    }

    @Override
    public String toString() {
        return String.format(
                "QueryTokenFactory{lsh-only, tables=%d, probeRange=%d}",
                numTables, probeRange
        );
    }
}