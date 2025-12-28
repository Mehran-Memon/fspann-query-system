package com.fspann.index.paper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * GFunctionRegistry - Centralized, Data-Adaptive GFunction Management (MSANNP)
 * ==========================================================================
 *
 * RESPONSIBILITY:
 * ---------------
 * 1. Build data-adaptive GFunctions using real dataset samples
 * 2. Cache them deterministically for (dimension, table, division)
 * 3. Provide INTEGER hash buckets (NOT BitSets)
 *
 * This class is the SINGLE source of truth for MSANNP hashing.
 *
 * IMPORTANT:
 * ----------
 * - BitSet-based coding is NOT supported here.
 * - All ANN logic must operate on integer bucket distances.
 *
 * THEORY (Peng et al., MSANNP):
 * ----------------------------
 * h(v) = floor((a · v + b) / ω)
 *
 * ω_j = (max_j − min_j) / 2^λ
 *
 * @version 3.0 (Option A, integer-hash only)
 */
public final class GFunctionRegistry {

    private static final Logger logger =
            LoggerFactory.getLogger(GFunctionRegistry.class);

    // Key: "dim_table_division" → GFunction
    private static final ConcurrentHashMap<String, Coding.GFunction> CACHE =
            new ConcurrentHashMap<>();

    // Global config (single-registry invariant)
    private static volatile boolean initialized = false;
    private static volatile int DIM = -1;
    private static volatile int M = -1;
    private static volatile int LAMBDA = -1;
    private static volatile long BASE_SEED = -1;
    private static volatile int TABLES = -1;
    private static volatile int DIVISIONS = -1;

    // Diagnostics
    private static volatile double omegaMin;
    private static volatile double omegaMax;
    private static volatile double omegaMean;

    private GFunctionRegistry() {}

    // ============================================================
    // INITIALIZATION
    // ============================================================

    public static synchronized void initialize(
            List<double[]> sample,
            int dimension,
            int m,
            int lambda,
            long baseSeed,
            int tables,
            int divisions
    ) {
        Objects.requireNonNull(sample, "sample");
        if (sample.isEmpty()) {
            throw new IllegalArgumentException("Sample vectors cannot be empty");
        }

        if (initialized &&
                DIM == dimension &&
                M == m &&
                LAMBDA == lambda &&
                BASE_SEED == baseSeed &&
                TABLES == tables &&
                DIVISIONS == divisions) {
            logger.info("GFunctionRegistry already initialized (same configuration)");
            return;
        }

        logger.info("Initializing GFunctionRegistry:");
        logger.info("  dim={} m={} λ={} tables={} divisions={} sampleSize={}",
                dimension, m, lambda, tables, divisions, sample.size());

        CACHE.clear();

        double[][] sampleArr = sample.toArray(new double[0][]);

        double minΩ = Double.MAX_VALUE;
        double maxΩ = Double.MIN_VALUE;
        double sumΩ = 0.0;
        int cntΩ = 0;

        for (int t = 0; t < tables; t++) {
            for (int d = 0; d < divisions; d++) {

                long seed = computeSeed(baseSeed, t, d);
                Coding.GFunction G =
                        Coding.buildFromSample(sampleArr, m, lambda, seed);

                CACHE.put(key(dimension, t, d), G);

                for (double ω : G.omega) {
                    minΩ = Math.min(minΩ, ω);
                    maxΩ = Math.max(maxΩ, ω);
                    sumΩ += ω;
                    cntΩ++;
                }
            }
        }

        DIM = dimension;
        M = m;
        LAMBDA = lambda;
        BASE_SEED = baseSeed;
        TABLES = tables;
        DIVISIONS = divisions;

        omegaMin = minΩ;
        omegaMax = maxΩ;
        omegaMean = sumΩ / cntΩ;
        initialized = true;

        logger.info(
                "GFunctionRegistry READY | GFunctions={} | ω[min={}, max={}, mean={}]",
                CACHE.size(),
                String.format("%.6f", omegaMin),
                String.format("%.6f", omegaMax),
                String.format("%.6f", omegaMean)
        );
    }

    // ============================================================
    // INTEGER HASHING (MSANNP CORE)
    // ============================================================

    /**
     * Compute integer hashes for all divisions of one table.
     */
    public static int[] hashForTable(double[] vec, int table) {
        validateVector(vec);

        int[] h = new int[DIVISIONS];
        for (int d = 0; d < DIVISIONS; d++) {
            Coding.GFunction G = get(vec.length, table, d);
            h[d] = Coding.H(vec, G);
        }
        return h;
    }

    /**
     * Compute integer hashes for all tables and divisions.
     * Shape: [table][division]
     */
    public static int[][] hashAllTables(double[] vec) {
        validateVector(vec);

        int[][] out = new int[TABLES][];
        for (int t = 0; t < TABLES; t++) {
            out[t] = hashForTable(vec, t);
        }
        return out;
    }

    // ============================================================
    // INTERNAL ACCESS
    // ============================================================

    public static Coding.GFunction get(int dimension, int table, int division) {
        if (!initialized) {
            throw new IllegalStateException("GFunctionRegistry not initialized");
        }
        if (dimension != DIM) {
            throw new IllegalArgumentException(
                    "Dimension mismatch: expected " + DIM + ", got " + dimension
            );
        }

        Coding.GFunction G = CACHE.get(key(dimension, table, division));
        if (G == null) {
            throw new IllegalStateException(
                    "Missing GFunction for table=" + table + ", division=" + division
            );
        }
        return G;
    }

    // ============================================================
    // VALIDATION & DIAGNOSTICS
    // ============================================================

    public static boolean isInitialized() {
        return initialized;
    }

    public static void validateVector(double[] vec) {
        if (!initialized) {
            throw new IllegalStateException("GFunctionRegistry not initialized");
        }
        if (vec == null) {
            throw new IllegalArgumentException("Vector is null");
        }
        if (vec.length != DIM) {
            throw new IllegalArgumentException(
                    "Vector dimension mismatch: expected " + DIM + ", got " + vec.length
            );
        }
        for (double v : vec) {
            if (!Double.isFinite(v)) {
                throw new IllegalArgumentException("Vector contains NaN/Inf");
            }
        }
    }

    public static Map<String, Object> getStats() {
        Map<String, Object> s = new LinkedHashMap<>();
        s.put("initialized", initialized);
        s.put("dimension", DIM);
        s.put("m", M);
        s.put("lambda", LAMBDA);
        s.put("tables", TABLES);
        s.put("divisions", DIVISIONS);
        s.put("cachedGFunctions", CACHE.size());
        s.put("omegaMin", omegaMin);
        s.put("omegaMax", omegaMax);
        s.put("omegaMean", omegaMean);
        return s;
    }

    public static synchronized void reset() {
        CACHE.clear();
        initialized = false;
        DIM = M = LAMBDA = TABLES = DIVISIONS = -1;
        BASE_SEED = -1;
        logger.warn("GFunctionRegistry RESET");
    }

    // ============================================================
    // UTILITIES
    // ============================================================

    private static String key(int dim, int table, int division) {
        return dim + "_" + table + "_" + division;
    }

    private static long computeSeed(long baseSeed, int table, int division) {
        return baseSeed + (table * 1_000_003L) + division;
    }
}
