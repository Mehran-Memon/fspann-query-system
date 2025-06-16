package com.fspann.index.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EvenLSH {
    private static final Logger logger = LoggerFactory.getLogger(EvenLSH.class);
    private final int dimensions;
    private final int numBuckets;
    private final double[][] projectionVectors; // Random vectors for projection

    public EvenLSH(int dimensions, int numBuckets) {
        this.dimensions = dimensions;
        this.numBuckets = numBuckets;
        this.projectionVectors = new double[numBuckets][dimensions];
        Random rand = new Random();
        for (int i = 0; i < numBuckets; i++) {
            for (int j = 0; j < dimensions; j++) {
                projectionVectors[i][j] = rand.nextGaussian(); // Random Gaussian values
            }
        }
    }


    public double project(double[] vector) {
        if (vector == null || vector.length != dimensions) {
            throw new IllegalArgumentException("Vector dimensions mismatch");
        }
        // Compute projection using the first random vector (simplified)
        double projection = 0.0;
        for (int i = 0; i < dimensions; i++) {
            projection += vector[i] * projectionVectors[0][i]; // Dot product with first vector
        }
        return projection;
    }

    public int getBucketId(double[] vector) {
        if (vector == null || vector.length != dimensions) {
            throw new IllegalArgumentException("Vector dimensions mismatch");
        }
        double projection = project(vector); // Use projection
        return Math.floorMod((int) (projection * numBuckets), numBuckets); // Hash to bucket
    }
    public List<Integer> getBuckets(double[] point, int range) {
        int mainBucket = getBucketId(point);
        return expandBuckets(mainBucket, range);
    }

    // Overload for QueryTokenFactory compatibility
    public List<Integer> getBuckets(double[] point) {
        return getBuckets(point, 2); // Default range, adjust based on expansionRange
    }
    /** number of input dimensions */
    public int getDimensions() {
        return dimensions;
    }

    /** number of buckets (shards) */
    public int getNumBuckets() {
        return numBuckets;
    }


    private List<Integer> expandBuckets(int mainBucket, int range) {
        List<Integer> buckets = new ArrayList<>();
        for (int i = -range; i <= range; i++) {
            int bucket = (mainBucket + i + numBuckets) % numBuckets; // Wrap around
            if (!buckets.contains(bucket)) {
                buckets.add(bucket);
            }
        }
        logger.debug("Expanded buckets for mainBucket {} with range {}: {}", mainBucket, range, buckets);
        return buckets;
    }
}