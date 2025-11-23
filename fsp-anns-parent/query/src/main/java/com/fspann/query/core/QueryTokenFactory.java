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
 * Builds QueryToken with:
 *  - per-table LSH bucket expansions (for legacy multiprobe path; optional)
 *  - paper codes: one BitSet per division (REQUIRED for partitioned mode)
 *
 * This factory runs on the trusted side (client / orchestrator) and
 * never embeds the plaintext vector in the token: only IV + ciphertext + codes.
 *
 * NOTE:
 *  - EvenLSH may be null in pure paper mode; in that case, we:
 *      * skip LSH bucket computation
 *      * emit empty bucket lists (size numTables, or empty if numTables==0)
 *      * still attach paper codes, which PartitionedIndexService uses.
 */
public class QueryTokenFactory {
    private static final Logger logger = LoggerFactory.getLogger(QueryTokenFactory.class);

    private final CryptoService cryptoService;
    private final KeyLifeCycleService keyService;

    /** May be null in pure paper mode. */
    private final EvenLSH lsh;

    // Multiprobe knobs (legacy LSH path)
    private final int numTables;       // can be 0 in paper-only mode
    @SuppressWarnings("unused")
    private final int expansionRange;  // unused in current paper mode

    // Paper knobs (must MATCH server's PartitionedIndexService)
    private final int divisions;  // ℓ
    private final int m;          // projections per division
    private final long seedBase;  // same seed used by server

    // ---------------------------------------------------------------------
    // Constructors
    // ---------------------------------------------------------------------

    /**
     * Config-driven constructor.
     *
     * @param lsh may be null in pure paper mode
     * @param numTables may be 0 (no LSH tables, paper-only)
     */
    public QueryTokenFactory(CryptoService cryptoService,
                             KeyLifeCycleService keyService,
                             EvenLSH lsh,
                             int expansionRange,
                             int numTables) {
        this.cryptoService = Objects.requireNonNull(cryptoService, "CryptoService must not be null");
        this.keyService    = Objects.requireNonNull(keyService, "KeyService must not be null");
        this.lsh           = lsh;  // may be null in paper-only configs

        if (numTables < 0) {
            throw new IllegalArgumentException("numTables must be >= 0");
        }
        if (expansionRange < 0) {
            throw new IllegalArgumentException("expansionRange must be >= 0");
        }

        this.numTables      = numTables;
        this.expansionRange = expansionRange;

        // Paper params from system properties (must match server config)
        this.divisions = Math.max(1, Integer.getInteger("paper.divisions", 9));
        this.m         = Math.max(1, Integer.getInteger("paper.m", 25));
        this.seedBase  = Long.getLong("paper.seed", 13L);

        int dimLog = (lsh != null ? lsh.getDimensions() : -1);
        String lshInfo = (lsh != null ? ("dim=" + dimLog) : "no-LSH");
        logger.info(
                "TokenFactory created: LSH[{}] divisions(ℓ)={} m={} numTables={} expansionRange={}",
                lshInfo, divisions, m, numTables, expansionRange
        );
    }

    /**
     * Convenience ctor: expansionRange defaults to 0.
     * Allows explicitly wiring paper params & optional LSH.
     *
     * @param lsh may be null in pure paper mode
     * @param numTables may be 0
     */
    public QueryTokenFactory(CryptoService cryptoService,
                             KeyLifeCycleService keyService,
                             EvenLSH lsh,
                             int numTables,
                             int divisions,
                             int m,
                             long seedBase) {
        this.cryptoService = Objects.requireNonNull(cryptoService, "CryptoService must not be null");
        this.keyService    = Objects.requireNonNull(keyService, "KeyService must not be null");
        this.lsh           = lsh;  // may be null

        if (numTables < 0) {
            throw new IllegalArgumentException("numTables must be >= 0");
        }
        if (divisions  <= 0) {
            throw new IllegalArgumentException("divisions must be > 0");
        }
        if (m          <= 0) {
            throw new IllegalArgumentException("m must be > 0");
        }

        this.numTables      = numTables;
        this.expansionRange = 0; // unused in paper mode

        this.divisions = divisions;
        this.m         = m;
        this.seedBase  = seedBase;

        int dimLog = (lsh != null ? lsh.getDimensions() : -1);
        String lshInfo = (lsh != null ? ("dim=" + dimLog) : "no-LSH");
        logger.info(
                "TokenFactory created: LSH[{}] divisions(ℓ)={} m={} numTables={} expansionRange={}",
                lshInfo, divisions, m, numTables, expansionRange
        );
    }

    // ---------------------------------------------------------------------
    // Core API
    // ---------------------------------------------------------------------

    /** Build a fresh token for a plaintext query vector. */
    public QueryToken create(double[] vector, int topK) {
        Objects.requireNonNull(vector, "vector");
        if (vector.length == 0) {
            throw new IllegalArgumentException("vector must be non-empty");
        }
        if (topK <= 0) {
            throw new IllegalArgumentException("topK must be > 0");
        }

        final int queryDim = vector.length;
        final int lshDim   = (lsh != null ? lsh.getDimensions() : 0);

        // Use LSH buckets only if we actually have an LSH and tables > 0
        boolean useLshBuckets = (lsh != null && numTables > 0);

        // If LSH dimension is known and mismatched, DO NOT throw – log and fall back.
        if (useLshBuckets && lshDim > 0 && lshDim != queryDim) {
            logger.warn(
                    "QueryTokenFactory.create: LSH dimension mismatch (lshDim={} vs queryDim={}); " +
                            "skipping LSH bucket computation and using dummy buckets. Partitioned codes (ℓ={}) still attached.",
                    lshDim, queryDim, divisions
            );
            useLshBuckets = false;
        }

        // -----------------------------
        // 1) Version/context + encrypt query
        // -----------------------------
        KeyVersion currentVersion = keyService.getCurrentVersion();
        SecretKey key    = currentVersion.getKey();
        int version      = currentVersion.getVersion();
        String encCtx    = "epoch_" + version + "_dim_" + queryDim;

        EncryptedPoint ep = cryptoService.encryptToPoint("query", vector, key);

        // -----------------------------
        // 2) Per-table bucket expansions (legacy LSH multiprobe path; optional)
        // -----------------------------
        final int probeHint = resolveProbeHint(topK);
        final List<List<Integer>> tableBuckets;

        if (useLshBuckets) {
            // Normal path: use LSH to compute per-table buckets
            List<List<Integer>> perTable = lsh.getBucketsForAllTables(vector, probeHint, numTables);
            List<List<Integer>> copy = new ArrayList<>(numTables);

            if (perTable == null || perTable.size() != numTables) {
                // Fallback: per-table buckets from basic API if multi-table helper not available
                for (int t = 0; t < numTables; t++) {
                    copy.add(new ArrayList<>(lsh.getBuckets(vector, topK, t)));
                }
            } else {
                for (List<Integer> l : perTable) {
                    copy.add(new ArrayList<>(l));
                }
            }
            tableBuckets = copy;
        } else {
            // No usable LSH (pure paper mode or dimension mismatch):
            // Provide deterministic dummy structure:
            //  - if numTables == 0: empty list
            //  - else: one empty list per table
            if (numTables <= 0) {
                tableBuckets = Collections.emptyList();
            } else {
                List<List<Integer>> dummy = new ArrayList<>(numTables);
                for (int t = 0; t < numTables; t++) {
                    dummy.add(Collections.emptyList());
                }
                tableBuckets = dummy;
            }
        }

        // -----------------------------
        // 3) REQUIRED for partitioned mode: attach codes (one BitSet per division)
        // -----------------------------
        BitSet[] codes = code(vector); // partitioned-paper coding; uses true dimension

        int totalProbes = tableBuckets.stream().mapToInt(List::size).sum();
        logger.debug(
                "Created token: dim={}, topK={}, tables={}, totalProbes={}, probeHint={}, hasCodes=true(ℓ={})",
                queryDim, topK, numTables, totalProbes, probeHint, divisions
        );

        // -----------------------------
        // 4) Build final token
        // -----------------------------
        return new QueryToken(
                tableBuckets,          // per-table buckets (may be empty)
                codes,                 // paper codes (ℓ BitSets)
                ep.getIv(),            // IV
                ep.getCiphertext(),    // encrypted query
                topK,
                numTables,
                encCtx,
                queryDim,              // actual vector dimension
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

    // ---------------------------------------------------------------------
    // probe.shards support
    // ---------------------------------------------------------------------

    /**
     * Resolve the effective probe hint for multiprobe LSH.
     *
     * Priority:
     *  1) System property "probe.shards" (set by K-adaptive loop in ForwardSecureANNSystem)
     *  2) Legacy heuristic based on numTables
     *
     * We clamp to a reasonable range to avoid pathological values:
     *  - At least max(numTables, topK)
     *  - At most 8192 (defensive upper bound)
     *
     * NOTE: Even if we end up not using LSH (pure paper mode), we still
     * compute this for logging consistency; it has no functional effect.
     */
    private int resolveProbeHint(int topK) {
        int fromProp = -1;
        String prop = System.getProperty("probe.shards");
        if (prop != null) {
            try {
                fromProp = Integer.parseInt(prop.trim());
            } catch (NumberFormatException ignore) {
                // fall through to fallback
            }
        }

        if (fromProp > 0) {
            int min = Math.max(1, Math.max(numTables, topK));
            int max = 8192;
            int resolved = Math.min(max, Math.max(min, fromProp));
            logger.trace("resolveProbeHint: using probe.shards={} -> resolved={} (min={}, max={})",
                    fromProp, resolved, min, max);
            return resolved;
        }

        // Fallback to original heuristic if no system property is set
        int fallback = Math.min(256, Math.max(32, Math.max(1, numTables * 2)));
        logger.trace("resolveProbeHint: no valid probe.shards; using fallback={}", fallback);
        return fallback;
    }

    // ---------------------------------------------------------------------
    // paper coding
    // ---------------------------------------------------------------------

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
