package com.fspann.index.paper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * GFunctionRegistry - Centralized, Data-Adaptive GFunction Management
 * ====================================================================
 *
 * PROBLEM SOLVED:
 * ---------------
 * The original Coding.code() created new GFunctions with hardcoded omega=1.0
 * on every call. This caused:
 *   - Wrong bucket widths for different datasets (SIFT, Glove, Deep1B, etc.)
 *   - Index/query GFunction mismatch (different random states)
 *   - ~0 candidates returned (hash collision probability ≈ 0)
 *
 * SOLUTION:
 * ---------
 * 1. Sample actual data during first batch insert
 * 2. Compute data-aware omega_j per projection based on actual ranges
 * 3. Cache GFunctions keyed by (dimension, table, division, seed)
 * 4. Guarantee index and query use IDENTICAL GFunction parameters
 *
 * USAGE:
 * ------
 * // During index build (first batch):
 * GFunctionRegistry.initialize(sampleVectors, dimension, m, lambda, baseSeed, tables, divisions);
 *
 * // During index/query:
 * GFunction G = GFunctionRegistry.get(dimension, table, division);
 * BitSet code = Coding.C(vector, G);
 *
 * THEORY (Peng et al.):
 * ---------------------
 * omega_j = (max_j - min_j) / 2^lambda
 *
 * This ensures each projection produces approximately 2^lambda distinct buckets,
 * giving collision probability ≈ 1/2^lambda per projection.
 *
 * For m projections with lambda bits each:
 *   P(full match) = (1/2^lambda)^m
 *   P(prefix match at depth d) = (1/2^lambda)^(m * (lambda-d) / lambda)
 *
 * @author FSP-ANN Team
 * @version 2.0 (Data-Adaptive)
 */
public final class GFunctionRegistry {

    private static final Logger logger = LoggerFactory.getLogger(GFunctionRegistry.class);

    // Key: "dim_table_division" -> GFunction
    private static final ConcurrentHashMap<String, Coding.GFunction> cache = new ConcurrentHashMap<>();

    // Global initialization state
    private static volatile boolean initialized = false;
    private static volatile int globalDimension = -1;
    private static volatile int globalM = -1;
    private static volatile int globalLambda = -1;
    private static volatile long globalBaseSeed = -1;
    private static volatile int globalTables = -1;
    private static volatile int globalDivisions = -1;

    // Statistics from initialization (for diagnostics)
    private static volatile double[] omegaStats = null;  // [min, max, mean] across all GFunctions

    private GFunctionRegistry() {}

    // =========================================================================
    // INITIALIZATION (call once during first batch insert)
    // =========================================================================

    /**
     * Initialize the registry with a sample of actual data vectors.
     *
     * This MUST be called before any indexing or querying.
     * Typically called with the first batch of vectors during index build.
     *
     * @param sample     Sample vectors from the dataset (recommend 1000-10000)
     * @param dimension  Vector dimensionality
     * @param m          Number of projections per GFunction
     * @param lambda     Bits per projection
     * @param baseSeed   Base random seed
     * @param tables     Number of LSH tables (L)
     * @param divisions  Number of divisions per table
     */
    public static synchronized void initialize(
            List<double[]> sample,
            int dimension,
            int m,
            int lambda,
            long baseSeed,
            int tables,
            int divisions
    ) {
        if (initialized && dimension == globalDimension && m == globalM
                && lambda == globalLambda && baseSeed == globalBaseSeed
                && tables == globalTables && divisions == globalDivisions) {
            logger.debug("GFunctionRegistry already initialized with same parameters");
            return;
        }
        logger.info(">>> GFunctionRegistry.initialize CALLED <<<");
        logger.info("  sample.size()={}, dimension={}, m={}, lambda={}",
                sample.size(), dimension, m, lambda);
        logger.info("  baseSeed={}, tables={}, divisions={}",
                baseSeed, tables, divisions);

        Objects.requireNonNull(sample, "sample cannot be null");
        if (sample.isEmpty()) {
            throw new IllegalArgumentException("sample cannot be empty");
        }
        if (dimension <= 0 || m <= 0 || lambda <= 0 || tables <= 0 || divisions <= 0) {
            throw new IllegalArgumentException("All parameters must be positive");
        }


        logger.info("Initializing GFunctionRegistry: dim={}, m={}, λ={}, tables={}, divisions={}, sampleSize={}",
                dimension, m, lambda, tables, divisions, sample.size());

        // Clear any existing cache
        cache.clear();

        // Convert sample to array for efficiency
        double[][] sampleArray = sample.toArray(new double[0][]);

        // Track omega statistics
        double minOmega = Double.MAX_VALUE;
        double maxOmega = Double.MIN_VALUE;
        double sumOmega = 0.0;
        int countOmega = 0;

        // Build GFunctions for all (table, division) combinations
        for (int t = 0; t < tables; t++) {
            for (int d = 0; d < divisions; d++) {
                long seed = computeSeed(baseSeed, t, d);
                String key = makeKey(dimension, t, d);

                Coding.GFunction G = Coding.buildFromSample(sampleArray, m, lambda, seed);
                cache.put(key, G);

                // Track omega stats
                for (double omega : G.omega) {
                    minOmega = Math.min(minOmega, omega);
                    maxOmega = Math.max(maxOmega, omega);
                    sumOmega += omega;
                    countOmega++;
                }

                logger.debug("Built GFunction: table={}, division={}, seed={}, avgOmega={}",
                        t, d, seed, Arrays.stream(G.omega).average().orElse(0.0));
            }
        }

        // Store global state
        globalDimension = dimension;
        globalM = m;
        globalLambda = lambda;
        globalBaseSeed = baseSeed;
        globalTables = tables;
        globalDivisions = divisions;
        omegaStats = new double[]{minOmega, maxOmega, sumOmega / countOmega};
        initialized = true;

        logger.info("GFunctionRegistry initialized: {} GFunctions cached, omega range=[{}, {}], mean={}",
                cache.size(),
                String.format("%.4f", minOmega),
                String.format("%.4f", maxOmega),
                String.format("%.4f", omegaStats[2]));
    }

    /**
     * Initialize with default sample size extraction from a larger batch.
     */
    public static void initializeFromBatch(
            List<double[]> batch,
            int dimension,
            int m,
            int lambda,
            long baseSeed,
            int tables,
            int divisions,
            int maxSampleSize
    ) {
        // Take a representative sample
        List<double[]> sample;
        if (batch.size() <= maxSampleSize) {
            sample = batch;
        } else {
            // Stratified sampling: take evenly spaced vectors
            sample = new ArrayList<>(maxSampleSize);
            double step = (double) batch.size() / maxSampleSize;
            for (int i = 0; i < maxSampleSize; i++) {
                int idx = (int) (i * step);
                sample.add(batch.get(idx));
            }
        }

        initialize(sample, dimension, m, lambda, baseSeed, tables, divisions);
    }

    // =========================================================================
    // RETRIEVAL (used by both index and query)
    // =========================================================================

    /**
     * Get the GFunction for a specific (table, division).
     *
     * @throws IllegalStateException if registry not initialized
     */
    public static Coding.GFunction get(int dimension, int table, int division) {
        if (!initialized) {
            throw new IllegalStateException(
                    "GFunctionRegistry not initialized! " +
                            "Call initialize() with sample data before indexing/querying."
            );
        }

        if (dimension != globalDimension) {
            throw new IllegalArgumentException(
                    "Dimension mismatch: registry initialized with " + globalDimension +
                            " but requested " + dimension
            );
        }

        String key = makeKey(dimension, table, division);
        Coding.GFunction G = cache.get(key);

        if (G == null) {
            throw new IllegalStateException(
                    "No GFunction cached for key=" + key +
                            ". Tables=" + globalTables + ", divisions=" + globalDivisions
            );
        }

        return G;
    }

    /**
     * Get GFunction using the standard seed computation.
     * This is the primary entry point for both index and query paths.
     */
    public static Coding.GFunction getBySeeds(int dimension, long baseSeed, int table, int division) {
        // Verify seed consistency
        if (initialized && baseSeed != globalBaseSeed) {
            logger.warn("Seed mismatch: registry baseSeed={}, requested baseSeed={}",
                    globalBaseSeed, baseSeed);
        }
        return get(dimension, table, division);
    }

    // =========================================================================
    // CODING WRAPPERS (drop-in replacements for Coding.code())
    // =========================================================================

    /**
     * Compute codes for all divisions of a single table.
     * Drop-in replacement for Coding.code(vec, divisions, m, lambda, seed).
     */
    public static BitSet[] codeForTable(double[] vec, int table) {
        if (!initialized) {
            throw new IllegalStateException("GFunctionRegistry not initialized");
        }

        BitSet[] codes = new BitSet[globalDivisions];
        for (int d = 0; d < globalDivisions; d++) {
            Coding.GFunction G = get(vec.length, table, d);
            codes[d] = Coding.C(vec, G);
        }
        return codes;
    }

    /**
     * Compute codes for all tables and divisions.
     * Returns [table][division] array of BitSets.
     */
    public static BitSet[][] codeAllTables(double[] vec) {
        if (!initialized) {
            throw new IllegalStateException("GFunctionRegistry not initialized");
        }

        BitSet[][] codes = new BitSet[globalTables][];
        for (int t = 0; t < globalTables; t++) {
            codes[t] = codeForTable(vec, t);
        }
        return codes;
    }

    // =========================================================================
    // UTILITIES
    // =========================================================================

    private static String makeKey(int dimension, int table, int division) {
        return dimension + "_" + table + "_" + division;
    }

    /**
     * Compute seed for (table, division) - MUST match PartitionedIndexService.tableSeed()
     */
    private static long computeSeed(long baseSeed, int table, int division) {
        // Match the seed computation in PartitionedIndexService:
        // tableSeed(table) = baseSeed + (table * 1_000_003L)
        // divisionSeed = tableSeed + division
        return baseSeed + (table * 1_000_003L) + division;
    }

    /**
     * Check if registry is initialized.
     */
    public static boolean isInitialized() {
        return initialized;
    }

    /**
     * Get initialization parameters (for diagnostics).
     */
    public static Map<String, Object> getStats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("initialized", initialized);
        stats.put("dimension", globalDimension);
        stats.put("m", globalM);
        stats.put("lambda", globalLambda);
        stats.put("baseSeed", globalBaseSeed);
        stats.put("tables", globalTables);
        stats.put("divisions", globalDivisions);
        stats.put("cachedGFunctions", cache.size());
        if (omegaStats != null) {
            stats.put("omegaMin", omegaStats[0]);
            stats.put("omegaMax", omegaStats[1]);
            stats.put("omegaMean", omegaStats[2]);
        }
        return stats;
    }

    /**
     * Reset the registry (for testing or re-initialization).
     */
    public static synchronized void reset() {
        cache.clear();
        initialized = false;
        globalDimension = -1;
        globalM = -1;
        globalLambda = -1;
        globalBaseSeed = -1;
        globalTables = -1;
        globalDivisions = -1;
        omegaStats = null;
        logger.info("GFunctionRegistry reset");
    }

    /**
     * Validate that a vector can be processed with current configuration.
     */
    public static void validateVector(double[] vec) {
        if (!initialized) {
            throw new IllegalStateException("GFunctionRegistry not initialized");
        }
        if (vec == null) {
            throw new IllegalArgumentException("Vector cannot be null");
        }
        if (vec.length != globalDimension) {
            throw new IllegalArgumentException(
                    "Vector dimension mismatch: expected " + globalDimension +
                            " but got " + vec.length
            );
        }
        for (double v : vec) {
            if (!Double.isFinite(v)) {
                throw new IllegalArgumentException("Vector contains NaN or Infinite values");
            }
        }
    }
}