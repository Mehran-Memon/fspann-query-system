package com.fspann.query.core;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;
import com.fspann.common.QueryToken;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.CryptoService;
import com.fspann.index.paper.EvenLSH;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.util.*;

/**
 * QueryTokenFactory (Option-C + Stabilization-Aware)
 * -------------------------------------------------
 * Responsibilities:
 *   • Encrypt query
 *   • Produce PAPER partition codes
 *   • Optionally generate LSH buckets
 *   • Respect stabilization for probe-range selection BUT
 *     NEVER modify candidate ordering or token codes
 */
public final class QueryTokenFactory {

    private static final Logger log = LoggerFactory.getLogger(QueryTokenFactory.class);

    private final CryptoService crypto;
    private final KeyLifeCycleService keyService;

    /** Optional LSH (null → codes-only mode) */
    private final EvenLSH lsh;

    private final int numTables;
    private final int probeRange;

    /** PAPER parameters */
    private final int divisions;
    private final int m;
    private final long seedBase;

    /** SystemConfig (includes stabilization config) */
    private final SystemConfig cfg;

    // ------------------------------------------------------------

    public QueryTokenFactory(
            CryptoService crypto,
            KeyLifeCycleService keyService,
            EvenLSH lsh,
            int numTables,
            int probeRange,
            int divisions,
            int m,
            long seedBase,
            SystemConfig cfg
    ) {
        this.crypto = Objects.requireNonNull(crypto, "crypto");
        this.keyService = Objects.requireNonNull(keyService, "keyService");

        if (divisions <= 0) throw new IllegalArgumentException("divisions > 0 required");
        if (m <= 0) throw new IllegalArgumentException("m > 0 required");
        if (numTables < 0) throw new IllegalArgumentException("numTables >= 0 required");
        if (probeRange < 0) throw new IllegalArgumentException("probeRange >= 0 required");

        this.lsh = lsh;  // nullable
        this.numTables = numTables;
        this.probeRange = probeRange;

        this.divisions = divisions;
        this.m = m;
        this.seedBase = seedBase;

        this.cfg = Objects.requireNonNull(cfg, "cfg");

        log.info(
                "TokenFactory: LSH={}, tables={}, divisions={}, m={}, seedBase={}, probeRange={}",
                (lsh != null ? "ON" : "OFF"),
                numTables,
                divisions,
                m,
                seedBase,
                probeRange
        );
    }

    // ------------------------------------------------------------
    // PUBLIC API
    // ------------------------------------------------------------

    public QueryToken create(double[] vec, int topK) {
        Objects.requireNonNull(vec, "query vector");
        if (topK <= 0) throw new IllegalArgumentException("topK must be > 0");

        if (vec == null || vec.length == 0) {
            throw new IllegalArgumentException("Invalid query vector");
        }

        final int dim = vec.length;

        /* 1) LSH buckets or empty tables */
        List<List<Integer>> buckets = (lsh == null)
                ? emptyTables(numTables)
                : computeBuckets(vec, topK, dim);

        /* 2) Encrypt query */
        KeyVersion kv = keyService.getCurrentVersion();
        SecretKey sk = kv.getKey();
        EncryptedPoint ep = crypto.encryptToPoint("query", vec, sk);

        /* 3) PAPER codes */
        BitSet[] codes = computeCodes(vec);

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

    // ------------------------------------------------------------
    // LSH BUCKET COMPUTATION (OPTIONAL)
    // ------------------------------------------------------------

    private List<List<Integer>> computeBuckets(double[] vec, int topK, int dim) {

        if (lsh.getDimensions() != dim) {
            throw new IllegalStateException(
                    "LSH dimension mismatch: LSH=" + lsh.getDimensions() +
                            ", query=" + dim +
                            " (Option-C forbids mismatch)"
            );
        }

        int probes = resolveProbeHint(topK);

        List<List<Integer>> out = new ArrayList<>(numTables);
        for (int t = 0; t < numTables; t++) {
            out.add(lsh.getBuckets(vec, probes, t));
        }
        return out;
    }

    private List<List<Integer>> emptyTables(int n) {
        List<List<Integer>> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) out.add(Collections.emptyList());
        return out;
    }

    /**
     * Stabilization-aware probe widening.
     * Does NOT change candidate filtering (done server-side).
     * Only influences LSH table probing depth.
     */
    private int resolveProbeHint(int topK) {
        SystemConfig.LshConfig lc = cfg.getLsh();
        int ps = (lc != null ? lc.getProbeShards() : 0);
        if (ps > 0) return Math.max(ps, topK);
        return Math.max(32, topK);
    }

    // ------------------------------------------------------------
    // PAPER PARTITION CODES (MANDATORY)
    // ------------------------------------------------------------

    private BitSet[] computeCodes(double[] vec) {
        BitSet[] out = new BitSet[divisions];

        for (int div = 0; div < divisions; div++) {
            BitSet bits = new BitSet(m);

            for (int proj = 0; proj < m; proj++) {

                long seed = mix64(
                        seedBase
                                ^ (div * 0x9E3779B97F4A7C15L)
                                ^ (proj * 0xBF58476D1CE4E5B9L)
                );

                double dot = 0.0;
                long s = seed;

                for (double x : vec) {
                    // xorshift
                    s ^= (s << 21);
                    s ^= (s >>> 35);
                    s ^= (s << 4);

                    // coefficient in [-1,1]
                    double r = ((s & 0x3fffffffL) / (double) 0x3fffffffL) * 2.0 - 1.0;
                    dot += x * r;
                }

                if (dot >= 0) bits.set(proj);
            }

            out[div] = bits;
        }
        return out;
    }

    // ------------------------------------------------------------
    // UTILS
    // ------------------------------------------------------------

    public boolean hasLSH() {
        return lsh != null;
    }

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

    private static long mix64(long z) {
        z = (z ^ (z >>> 33)) * 0xff51afd7ed558ccdL;
        z = (z ^ (z >>> 33)) * 0xc4ceb9fe1a85ec53L;
        return z ^ (z >>> 33);
    }
}
