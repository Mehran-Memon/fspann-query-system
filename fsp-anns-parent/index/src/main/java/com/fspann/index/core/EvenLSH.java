package com.fspann.index.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * EvenLSH projects vectors using random Gaussian vectors for bucketization.
 * Suitable for high-dimensional data.
 */
public class EvenLSH {
    private static final Logger logger = LoggerFactory.getLogger(EvenLSH.class);

    private final int dimensions;
    private final int numBuckets;
    private final double[][] projectionVectors; // One vector per bucket

    public EvenLSH(int dimensions, int numBuckets) {
        if (dimensions <= 0 || numBuckets <= 0) {
            throw new IllegalArgumentException("Dimensions and bucket count must be positive");
        }
        this.dimensions = dimensions;
        this.numBuckets = numBuckets;
        this.projectionVectors = new double[numBuckets][dimensions];

        Random rand = new Random();
        for (int i = 0; i < numBuckets; i++) {
            for (int j = 0; j < dimensions; j++) {
                projectionVectors[i][j] = rand.nextGaussian();
            }
        }
    }

    /**
     * Projects a vector using the first projection vector only.
     * Used to simplify dimensional projection.
     */
    public double project(double[] vector) {
        validateVector(vector);
        double projection = 0.0;
        for (int i = 0; i < dimensions; i++) {
            projection += vector[i] * projectionVectors[0][i];
        }
        return projection;
    }

    /**
     * Returns the main bucket for a given vector.
     */
    public int getBucketId(double[] vector) {
        validateVector(vector);
        double projection = project(vector);
        return Math.floorMod((int) (projection * numBuckets), numBuckets);
    }

    /**
     * Expands the main bucket into a range of nearby buckets (±range).
     */
    public List<Integer> getBuckets(double[] point, int range) {
        int mainBucket = getBucketId(point);
        return expandBuckets(mainBucket, range);
    }

    /**
     * Default range = 2. Used for backward compatibility.
     */
    public List<Integer> getBuckets(double[] point) {
        return getBuckets(point, 2);
    }

    public int getDimensions() {
        return dimensions;
    }

    public int getNumBuckets() {
        return numBuckets;
    }

    private List<Integer> expandBuckets(int mainBucket, int range) {
        List<Integer> buckets = new ArrayList<>();
        for (int i = -range; i <= range; i++) {
            int bucket = (mainBucket + i + numBuckets) % numBuckets;
            if (!buckets.contains(bucket)) {
                buckets.add(bucket);
            }
        }
        logger.debug("Expanded buckets for mainBucket {} with range {}: {}", mainBucket, range, buckets);
        return buckets;
    }

    private void validateVector(double[] vector) {
        if (vector == null || vector.length != dimensions) {
            throw new IllegalArgumentException("Vector dimensions mismatch: expected=" + dimensions + ", got=" + (vector == null ? 0 : vector.length));
        }
    }
}
