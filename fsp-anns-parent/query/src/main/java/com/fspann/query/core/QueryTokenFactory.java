package com.fspann.query.core;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.KeyLifeCycleService;
import com.fspann.common.KeyVersion;
import com.fspann.common.QueryToken;
import com.fspann.crypto.CryptoService;
import com.fspann.index.paper.EvenLSH;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.util.*;

/**
 * QueryTokenFactory (Option-C: Final SANNP/mSANNP Version)
 * --------------------------------------------------------
 * Rules:
 *   • Always produce partition codes BitSet[ℓ] using (m, seedBase).
 *   • LSH is OPTIONAL. Only used if SecureLSHIndexService.registerLsh() supplied one.
 *   • STRICT dimension matching: LSH.getDimensions() == queryDim, else fail hard.
 *   • No dimension mismatch fallback.
 *   • No legacy probe/r logic.
 *   • K-adaptive controlled via probe.shards.
 *
 * Outputs:
 *   • QueryToken with encrypted query ciphertext + IV
 *   • Partition codes (mandatory)
 *   • Optional LSH buckets (if LSH present)
 */
public final class QueryTokenFactory {

    private static final Logger log = LoggerFactory.getLogger(QueryTokenFactory.class);

    private final CryptoService crypto;
    private final KeyLifeCycleService keyService;

    /** OPTIONAL: per-dimension LSH. If null → codes-only mode. */
    private final EvenLSH lsh;

    private final int numTables;     // LSH tables (if LSH exists)
    private final int probeRange;    // additional probe depth == ignored unless LSH != null

    /** PAPER parameters — MUST match PartitionedIndexService */
    private final int divisions;     // ℓ
    private final int m;             // projections per division
    private final long seedBase;     // global seed for deterministic coding

    // ------------------------------------------------------------

    public QueryTokenFactory(
            CryptoService crypto,
            KeyLifeCycleService keyService,
            EvenLSH lsh,               // may be null (codes-only mode)
            int numTables,
            int probeRange,
            int divisions,
            int m,
            long seedBase
    ) {
        this.crypto = Objects.requireNonNull(crypto, "crypto");
        this.keyService = Objects.requireNonNull(keyService, "keyService");

        if (divisions <= 0) throw new IllegalArgumentException("divisions must be > 0");
        if (m <= 0) throw new IllegalArgumentException("m must be > 0");
        if (numTables < 0) throw new IllegalArgumentException("numTables >= 0 required");
        if (probeRange < 0) throw new IllegalArgumentException("probeRange >= 0 required");

        this.lsh = lsh;                       // nullable
        this.numTables = numTables;
        this.probeRange = probeRange;

        this.divisions = divisions;
        this.m = m;
        this.seedBase = seedBase;

        log.info("Option-C TokenFactory: LSH={}, tables={}, divisions={}, m={}, seedBase={}",
                (lsh != null ? "ON" : "OFF"), numTables, divisions, m, seedBase);
    }

    // ------------------------------------------------------------
    // PUBLIC API
    // ------------------------------------------------------------

    public QueryToken create(double[] vec, int topK) {
        Objects.requireNonNull(vec, "query vector");
        if (topK <= 0) throw new IllegalArgumentException("topK must be > 0");

        final int dim = vec.length;

        // ---- 1) Prepare LSH buckets (optional)
        List<List<Integer>> buckets;
        if (lsh == null) {
            buckets = emptyTables(numTables);

        } else {
            // Option-C forbids mismatched dims
            if (lsh.getDimensions() != dim) {
                throw new IllegalStateException(
                        "LSH dimension mismatch: LSH dim=" + lsh.getDimensions()
                                + " query=" + dim
                                + " (Option-C prohibits mismatch)"
                );
            }
            buckets = computeBuckets(vec, topK);
        }

        // ---- 2) Encrypt query vector (AES-GCM)
        KeyVersion kv = keyService.getCurrentVersion();
        SecretKey sk = kv.getKey();
        EncryptedPoint ep = crypto.encryptToPoint("query", vec, sk);

        // ---- 3) PAPER partition codes (mandatory)
        BitSet[] codes = computeCodes(vec);

        // ---- 4) Build and return immutable token
        return new QueryToken(
                buckets,                // LSH buckets (possibly empty)
                codes,                  // PAPER codes (mandatory)
                ep.getIv(),             // IV
                ep.getCiphertext(),     // ciphertext
                topK,
                numTables,
                "dim_" + dim + "_v" + kv.getVersion(),
                dim,
                kv.getVersion()
        );
    }

    // ------------------------------------------------------------
    // LSH BUCKETS (OPTIONAL)
    // ------------------------------------------------------------

    private List<List<Integer>> computeBuckets(double[] vec, int topK) {
        if (lsh == null || numTables == 0) {
            return emptyTables(numTables);
        }

        int probes = resolveProbeHint(topK);

        List<List<Integer>> out = new ArrayList<>(numTables);
        for (int t = 0; t < numTables; t++) {
            List<Integer> ids = lsh.getBuckets(vec, probes, t);
            out.add(new ArrayList<>(ids));
        }
        return out;
    }

    private List<List<Integer>> emptyTables(int n) {
        List<List<Integer>> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            out.add(Collections.emptyList());
        }
        return out;
    }

    /** K-adaptive probe widening (same policy as PartitionedIndexService). */
    private int resolveProbeHint(int topK) {
        String v = System.getProperty("probe.shards");
        if (v != null) {
            try {
                int p = Integer.parseInt(v.trim());
                return Math.max(p, topK);
            } catch (Exception ignored) {}
        }
        return Math.max(32, topK);
    }

    // ------------------------------------------------------------
    // PAPER PARTITION CODES (MANDATORY)
    // MUST MATCH PartitionedIndexService.code()
    // ------------------------------------------------------------

    private BitSet[] computeCodes(double[] vec) {
        BitSet[] out = new BitSet[divisions];

        for (int div = 0; div < divisions; div++) {
            BitSet bits = new BitSet(m);

            for (int proj = 0; proj < m; proj++) {

                long seed = mix64(
                        seedBase
                                ^ ((long) div * 0x9E3779B97F4A7C15L)
                                ^ ((long) proj * 0xBF58476D1CE4E5B9L)
                );

                double dot = 0.0;
                long s = seed;

                for (double x : vec) {
                    // xorshift transform
                    s ^= (s << 21);
                    s ^= (s >>> 35);
                    s ^= (s << 4);

                    // pseudo-random coefficient r ∈ [-1,1]
                    double r = ((s & 0x3fffffffL) / (double) 0x3fffffffL) * 2.0 - 1.0;
                    dot += x * r;
                }

                if (dot >= 0) {
                    bits.set(proj);
                }
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
