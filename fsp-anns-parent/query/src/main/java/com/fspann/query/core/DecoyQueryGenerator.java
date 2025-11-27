package com.fspann.query.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates synthetic decoy queries to obfuscate access patterns.
 *
 * Strategy:
 * - Mix real queries with synthetic decoys at configurable ratio
 * - Decoys use same encryption/token generation as real queries
 * - Results from decoys are discarded (never returned to client)
 */
public class DecoyQueryGenerator {
    private static final Logger logger = LoggerFactory.getLogger(DecoyQueryGenerator.class);

    private final int dimension;
    private final SecureRandom random;
    private final double decoyRatio; // 0.0 to 1.0
    private final DecoyDistribution distribution;

    public enum DecoyDistribution {
        UNIFORM,    // Random uniform vectors
        GAUSSIAN,   // Gaussian-distributed components
        CLUSTERED   // Vectors clustered around dataset centroids
    }

    /**
     * @param dimension Vector dimensionality
     * @param decoyRatio Proportion of decoys to inject (0.0 = none, 0.5 = 1 decoy per real query)
     * @param distribution Distribution strategy for decoy vectors
     */
    public DecoyQueryGenerator(int dimension,
                               double decoyRatio,
                               DecoyDistribution distribution) {
        if (dimension <= 0) {
            throw new IllegalArgumentException("dimension must be > 0");
        }
        if (decoyRatio < 0.0 || decoyRatio > 1.0) {
            throw new IllegalArgumentException("decoyRatio must be in [0.0, 1.0]");
        }

        this.dimension = dimension;
        this.decoyRatio = decoyRatio;
        this.distribution = distribution;
        this.random = new SecureRandom();

        logger.info("DecoyQueryGenerator initialized: dim={}, ratio={}, distribution={}",
                dimension, decoyRatio, distribution);
    }

    /**
     * Generates a single synthetic decoy query vector.
     */
    public double[] generateDecoy() {
        return switch (distribution) {
            case UNIFORM -> generateUniform();
            case GAUSSIAN -> generateGaussian();
            case CLUSTERED -> generateClustered();
        };
    }

    /**
     * For a batch of N real queries, determine how many decoys to inject.
     */
    public int computeDecoyCount(int realQueryCount) {
        if (decoyRatio <= 0.0) return 0;

        // Poisson-like distribution to avoid fixed patterns
        double expected = realQueryCount * decoyRatio;
        int count = (int) Math.round(expected);

        // Add small random jitter (±20%)
        int jitter = (int) (count * 0.2 * (random.nextDouble() - 0.5));
        count = Math.max(0, count + jitter);

        return count;
    }

    /**
     * Interleaves decoys with real queries to obscure which are genuine.
     *
     * @param realQueries Original query vectors
     * @return Mixed list with decoys inserted at random positions
     */
    public List<QueryWrapper> interleaveDecoys(List<double[]> realQueries) {
        int numReal = realQueries.size();
        int numDecoys = computeDecoyCount(numReal);

        List<QueryWrapper> mixed = new ArrayList<>(numReal + numDecoys);

        // Add all real queries
        for (int i = 0; i < numReal; i++) {
            mixed.add(new QueryWrapper(realQueries.get(i), false, i));
        }

        // Add decoys
        for (int i = 0; i < numDecoys; i++) {
            mixed.add(new QueryWrapper(generateDecoy(), true, -1));
        }

        // Shuffle to obscure pattern
        shuffle(mixed);

        logger.debug("Interleaved {} real queries with {} decoys", numReal, numDecoys);
        return mixed;
    }

    // --- Distribution Strategies ---

    private double[] generateUniform() {
        double[] vec = new double[dimension];
        for (int i = 0; i < dimension; i++) {
            vec[i] = random.nextDouble() * 2.0 - 1.0; // [-1, 1]
        }
        normalize(vec);
        return vec;
    }

    private double[] generateGaussian() {
        double[] vec = new double[dimension];
        for (int i = 0; i < dimension; i++) {
            vec[i] = random.nextGaussian();
        }
        normalize(vec);
        return vec;
    }

    private double[] generateClustered() {
        // Simplified: same as Gaussian for now
        // In production, would cluster around dataset statistics
        return generateGaussian();
    }

    private void normalize(double[] vec) {
        double norm = 0.0;
        for (double v : vec) {
            norm += v * v;
        }
        norm = Math.sqrt(Math.max(1e-12, norm));
        for (int i = 0; i < vec.length; i++) {
            vec[i] /= norm;
        }
    }

    private void shuffle(List<QueryWrapper> list) {
        for (int i = list.size() - 1; i > 0; i--) {
            int j = ThreadLocalRandom.current().nextInt(i + 1);
            QueryWrapper temp = list.get(i);
            list.set(i, list.get(j));
            list.set(j, temp);
        }
    }

    /**
     * Wrapper to track whether a query is real or decoy.
     */
    public static class QueryWrapper {
        public final double[] vector;
        public final boolean isDecoy;
        public final int originalIndex; // -1 for decoys

        public QueryWrapper(double[] vector, boolean isDecoy, int originalIndex) {
            this.vector = vector;
            this.isDecoy = isDecoy;
            this.originalIndex = originalIndex;
        }
    }
}
