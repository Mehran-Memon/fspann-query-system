package com.fspann.index.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * EvenLSH projects vectors using random Gaussian vectors for bucketization.
 * Optimized for arbitrary-dimensional data with efficient dot product and adaptive projection vectors.
 */
public class EvenLSH {
    private static final Logger logger = LoggerFactory.getLogger(EvenLSH.class);
    private final ConcurrentMap<String, CacheEntry> cachedProjections = new ConcurrentHashMap<>();
    private final int dimensions;
    private final int numBuckets;
    private final int numProjections;
    private final double[][] projectionVectors;
    private static final long CACHE_EXPIRY_MS = 10 * 60 * 1000; // 10 minutes
    private static final int UNROLL_THRESHOLD = 256; // Threshold for loop unrolling
    private static final int MAX_CACHE_SIZE = 20000; // Cache size for large datasets

    private static class CacheEntry {
        final double projection;
        final long timestamp;

        CacheEntry(double projection) {
            this.projection = projection;
            this.timestamp = System.currentTimeMillis();
        }
    }

    public EvenLSH(int dimensions, int numBuckets) {
        this(dimensions, numBuckets, calculateNumProjections(dimensions, numBuckets));
    }

    public EvenLSH(int dimensions, int numBuckets, int numProjections) {
        if (dimensions <= 0 || numBuckets <= 0 || numProjections <= 0) {
            throw new IllegalArgumentException("Dimensions, bucket count, and projection count must be positive");
        }
        this.dimensions = dimensions;
        this.numBuckets = numBuckets;
        this.numProjections = Math.max(numBuckets, Math.min(numProjections, numBuckets * 8));
        this.projectionVectors = new double[this.numProjections][dimensions];

        Random rand = new Random();
        for (int i = 0; i < this.numProjections; i++) {
            double norm = 0.0;
            for (int j = 0; j < dimensions; j++) {
                projectionVectors[i][j] = rand.nextGaussian();
                norm += projectionVectors[i][j] * projectionVectors[i][j];
            }
            norm = Math.sqrt(norm);
            for (int j = 0; j < dimensions; j++) {
                projectionVectors[i][j] /= norm;
            }
        }
        logger.info("Initialized EvenLSH with {} dimensions, {} buckets, {} projections", dimensions, numBuckets, numProjections);
    }

    /**
     * Calculates adaptive number of projections based on dimensions and buckets.
     */
    private static int calculateNumProjections(int dimensions, int numBuckets) {
        // Scale projections with log(dimensions) for arbitrary dimensions
        int projections = (int) Math.ceil(numBuckets * Math.log(Math.max(dimensions, 1) / 16.0) / Math.log(2));
        return Math.max(numBuckets, Math.min(projections, numBuckets * 8));
    }

    /**
     * Projects a vector using the specified projection vector.
     */
    public double project(double[] vector, int tableIndex, String id) {
        validateVector(vector);
        Objects.requireNonNull(id, "ID cannot be null");

        cleanExpiredCache();
        return cachedProjections.computeIfAbsent(id, k -> {
            long start = System.nanoTime();
            double projection = computeDotProduct(vector, projectionVectors[tableIndex % numProjections]);
            long duration = System.nanoTime() - start;
            logger.debug("Computed projection for id={}, dim={}, duration={} ns", id, dimensions, duration);
            return new CacheEntry(projection);
        }).projection;
    }

    /**
     * Default projection for backward compatibility.
     */
    public double project(double[] vector) {
        return project(vector, 0, UUID.randomUUID().toString());
    }

    /**
     * Returns the main bucket for a given vector.
     */
    public int getBucketId(double[] vector) {
        validateVector(vector);
        long start = System.nanoTime();
        double projection = project(vector);
        int bucketId = Math.floorMod((int) (projection * numBuckets), numBuckets);
        long duration = System.nanoTime() - start;
        logger.debug("Computed bucketId={} for dim={}, duration={} ns", bucketId, dimensions, duration);
        return bucketId;
    }

    /**
     * Expands the main bucket into a range of nearby buckets based on top-K.
     */
    public List<Integer> getBuckets(double[] point, int topK) {
        int range = calculateBucketRange(topK);
        int mainBucket = getBucketId(point);
        return expandBuckets(mainBucket, range);
    }

    /**
     * Default range for backward compatibility.
     */
    public List<Integer> getBuckets(double[] point) {
        return getBuckets(point, DEFAULT_TOP_K);
    }

    public int getDimensions() {
        return dimensions;
    }

    public int getNumBuckets() {
        return numBuckets;
    }

    /**
     * Clears the projection cache, e.g., during key rotation.
     */
    public void clearCache() {
        cachedProjections.clear();
        logger.info("Cleared projection cache for dim={}", dimensions);
    }

    private List<Integer> expandBuckets(int mainBucket, int range) {
        List<Integer> buckets = new ArrayList<>();
        for (int i = -range; i <= range; i++) {
            int bucket = (mainBucket + i + numBuckets) % numBuckets;
            if (!buckets.contains(bucket)) {
                buckets.add(bucket);
            }
        }
        logger.debug("Expanded buckets for mainBucket={}, range={}, dim={}, result={}", mainBucket, range, dimensions, buckets);
        return buckets;
    }

    private int calculateBucketRange(int topK) {
        int range = Math.max(1, (int) Math.ceil(Math.log(topK + 1) / Math.log(2)));
        range = Math.min(range, numBuckets / 4); // Cap range to avoid excessive buckets
        logger.debug("Calculated bucket range={} for topK={}, dim={}", range, topK, dimensions);
        return range;
    }

    private double computeDotProduct(double[] vector, double[] projection) {
        if (dimensions <= UNROLL_THRESHOLD) {
            // Unrolled loop for smaller dimensions
            double sum = 0.0;
            int i = 0;
            for (; i <= dimensions - 4; i += 4) {
                sum += vector[i] * projection[i] +
                        vector[i + 1] * projection[i + 1] +
                        vector[i + 2] * projection[i + 2] +
                        vector[i + 3] * projection[i + 3];
            }
            for (; i < dimensions; i++) {
                sum += vector[i] * projection[i];
            }
            return sum;
        } else {
            // Block-based computation for large dimensions
            double sum = 0.0;
            int blockSize = 8;
            int i = 0;
            for (; i <= dimensions - blockSize; i += blockSize) {
                for (int j = 0; j < blockSize; j++) {
                    sum += vector[i + j] * projection[i + j];
                }
            }
            for (; i < dimensions; i++) {
                sum += vector[i] * projection[i];
            }
            return sum;
        }
    }

    private void cleanExpiredCache() {
        long now = System.currentTimeMillis();
        int initialSize = cachedProjections.size();
        if (initialSize > MAX_CACHE_SIZE) {
            cachedProjections.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue((e1, e2) -> Long.compare(e1.timestamp, e2.timestamp)))
                    .limit(initialSize - MAX_CACHE_SIZE / 2)
                    .forEach(entry -> cachedProjections.remove(entry.getKey()));
        }
        cachedProjections.entrySet().removeIf(entry -> now - entry.getValue().timestamp > CACHE_EXPIRY_MS);
        if (initialSize != cachedProjections.size()) {
            logger.debug("Cleaned cache for dim={}, size reduced from {} to {}", dimensions, initialSize, cachedProjections.size());
        }
    }

    private void validateVector(double[] vector) {
        if (vector == null || vector.length != dimensions) {
            throw new IllegalArgumentException("Vector dimensions mismatch: expected=" + dimensions + ", got=" + (vector == null ? 0 : vector.length));
        }
        for (double v : vector) {
            if (Double.isNaN(v) || Double.isInfinite(v)) {
                throw new IllegalArgumentException("Vector contains invalid values (NaN or Infinite)");
            }
        }
    }

    private static final int DEFAULT_TOP_K = 5;
}