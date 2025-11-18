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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

/**
 * Builds QueryToken with:
 *  - per-table LSH bucket expansions (for legacy multiprobe path)
 *  - paper codes: one BitSet per division (REQUIRED for partitioned mode)
 *
 * This factory runs on the trusted side (client / orchestrator) and
 * never embeds the plaintext vector in the token: only IV + ciphertext + codes.
 */
public class QueryTokenFactory {
    private static final Logger logger = LoggerFactory.getLogger(QueryTokenFactory.class);

    private final CryptoService cryptoService;
    private final KeyLifeCycleService keyService;
    private final EvenLSH lsh;

    // Multiprobe knobs
    private final int numTables;
    @SuppressWarnings("unused")
    private final int expansionRange;

    // Paper knobs (must MATCH server's PartitionedIndexService)
    private final int divisions;  // ℓ
    private final int m;          // projections per division
    private final long seedBase;  // same seed used by server

    public QueryTokenFactory(CryptoService cryptoService,
                             KeyLifeCycleService keyService,
                             EvenLSH lsh,
                             int expansionRange,
                             int numTables) {
        this.cryptoService = Objects.requireNonNull(cryptoService, "CryptoService must not be null");
        this.keyService    = Objects.requireNonNull(keyService, "KeyService must not be null");
        this.lsh           = Objects.requireNonNull(lsh, "EvenLSH must not be null");

        if (numTables <= 0) throw new IllegalArgumentException("numTables must be positive");
        if (expansionRange < 0) throw new IllegalArgumentException("expansionRange must be >= 0");

        this.numTables      = numTables;
        this.expansionRange = expansionRange;

        // Paper params from system properties (must match server config)
        this.divisions = Math.max(1, Integer.getInteger("paper.divisions", 9));
        this.m         = Math.max(1, Integer.getInteger("paper.m", 25));
        this.seedBase  = Long.getLong("paper.seed", 13L);

        logger.info("TokenFactory created: dim={} divisions(ℓ)={} m={} numTables={} shardsToProbe={}",
                lsh.getDimensions(), divisions, m, numTables, expansionRange);
    }

    /** Convenience ctor: expansionRange defaults to 0. */
    public QueryTokenFactory(CryptoService cryptoService,
                             KeyLifeCycleService keyService,
                             EvenLSH lsh,
                             int numTables,
                             int divisions,
                             int m,
                             long seedBase) {
        this.cryptoService = Objects.requireNonNull(cryptoService, "CryptoService must not be null");
        this.keyService    = Objects.requireNonNull(keyService, "KeyService must not be null");
        this.lsh           = Objects.requireNonNull(lsh, "EvenLSH must not be null");

        if (numTables <= 0) throw new IllegalArgumentException("numTables must be positive");
        if (divisions  <= 0) throw new IllegalArgumentException("divisions must be > 0");
        if (m          <= 0) throw new IllegalArgumentException("m must be > 0");

        this.numTables      = numTables;
        this.expansionRange = 0; // unused in paper mode

        this.divisions = divisions;
        this.m         = m;
        this.seedBase  = seedBase;

        logger.info("TokenFactory created: dim={} divisions(ℓ)={} m={} numTables={} shardsToProbe={}",
                lsh.getDimensions(), divisions, m, numTables, expansionRange);
    }

    /** Build a fresh token for a plaintext query vector. */
    public QueryToken create(double[] vector, int topK) {
        Objects.requireNonNull(vector, "vector");
        if (vector.length == 0) throw new IllegalArgumentException("vector must be non-empty");
        if (topK <= 0) throw new IllegalArgumentException("topK must be > 0");

        int lshDim = lsh.getDimensions();
        if (lshDim > 0 && lshDim != vector.length) {
            throw new IllegalArgumentException("Vector dimension mismatch: expected " + lshDim + " but got " + vector.length);
        }

        // Version/context + encrypt query
        KeyVersion currentVersion = keyService.getCurrentVersion();
        SecretKey key    = currentVersion.getKey();
        int version      = currentVersion.getVersion();
        String encCtx    = "epoch_" + version + "_dim_" + vector.length;

        EncryptedPoint ep = cryptoService.encryptToPoint("query", vector, key);

        // Per-table bucket expansions for multiprobe path
        final int probeHint = Math.min(256, Math.max(32, numTables * 2));
        List<List<Integer>> perTable = lsh.getBucketsForAllTables(vector, probeHint, numTables);
        List<List<Integer>> copy = new ArrayList<>(numTables);
        if (perTable == null || perTable.size() != numTables) {
            for (int t = 0; t < numTables; t++) {
                copy.add(new ArrayList<>(lsh.getBuckets(vector, topK, t)));
            }
        } else {
            for (List<Integer> l : perTable) copy.add(new ArrayList<>(l));
        }

        // REQUIRED for partitioned mode: attach codes (one BitSet per division)
        BitSet[] codes = code(vector);

        int totalProbes = copy.stream().mapToInt(List::size).sum();
        logger.debug("Created token: dim={}, topK={}, tables={}, totalProbes={}, hasCodes=true(ℓ={})",
                vector.length, topK, numTables, totalProbes, divisions);

        return new QueryToken(
                copy,                 // per-table buckets
                codes,                // paper codes (ℓ BitSets)
                ep.getIv(),           // IV
                ep.getCiphertext(),   // encrypted query
                topK,
                numTables,
                encCtx,
                vector.length,
                version
        );
    }

    /**
     * Derive a new token with a different topK from an existing token.
     *
     * NOTE:
     *  - We reuse per-table buckets, codes, IV and ciphertext.
     *  - This avoids needing plaintext on the server side.
     *  - If you ever want "true" re-bucketing at new K, that must be done
     *    on the client/orchestrator which still has the plaintext vector.
     */
    public QueryToken derive(QueryToken base, int newTopK) {
        Objects.requireNonNull(base, "base");
        if (newTopK <= 0) throw new IllegalArgumentException("newTopK must be > 0");

        logger.debug("Deriving token from base: oldK={}, newK={}, dim={}, tables={}",
                base.getTopK(), newTopK, base.getDimension(), base.getNumTables());

        return new QueryToken(
                base.getTableBuckets(),      // reuse buckets
                base.getCodes(),             // reuse codes
                base.getIv(),                // reuse IV
                base.getEncryptedQuery(),    // reuse ciphertext
                newTopK,
                base.getNumTables(),
                base.getEncryptionContext(),
                base.getDimension(),
                base.getVersion()
        );
    }

    /* ----------------------- paper coding ----------------------- */

    /**
     * Deterministic coding; MUST match PartitionedIndexService.code(...).
     * Produces ℓ BitSets, each of length m. Bit j is set iff signed projection >= 0.
     */
    private BitSet[] code(double[] vec) {
        Objects.requireNonNull(vec, "vec");
        BitSet[] out = new BitSet[divisions];
        for (int div = 0; div < divisions; div++) {
            BitSet bits = new BitSet(m);
            for (int proj = 0; proj < m; proj++) {
                long seed = mix64(seedBase
                        ^ ((long) div * 0x9E3779B97F4A7C15L)
                        ^ ((long) proj * 0xBF58476D1CE4E5B9L));
                double dot = 0.0;
                long s = seed;
                for (double v : vec) {
                    // xorshift-ish update
                    s ^= (s << 21);
                    s ^= (s >>> 35);
                    s ^= (s << 4);
                    // map to [-1, 1]
                    double r = ((s & 0x3fffffffL) / (double) 0x3fffffffL) * 2.0 - 1.0;
                    dot += v * r;
                }
                if (dot >= 0) bits.set(proj);
            }
            out[div] = bits;
        }
        return out;
    }

    private static long mix64(long z) {
        z = (z ^ (z >>> 33)) * 0xff51afd7ed558ccdl;
        z = (z ^ (z >>> 33)) * 0xc4ceb9fe1a85ec53l;
        return z ^ (z >>> 33);
    }
}
