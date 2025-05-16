package com.fspann.index.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.*;

/**
 * EvenLSH—cosine-projection / dynamic-quantile LSH
 */
public class EvenLSH {
    private static final Logger logger = LoggerFactory.getLogger(EvenLSH.class);

    private double[] projectionVector;
    private double[] criticalAngles;
    private final int dimensions;
    private final int numBuckets;

    /**
     * @param dimensions dimensionality of vectors
     * @param numBuckets desired number of buckets
     */
    public EvenLSH(int dimensions, int numBuckets) {
        if (dimensions <= 0) throw new IllegalArgumentException("dimensions must be positive");
        if (numBuckets < 2) throw new IllegalArgumentException("numBuckets must be >= 2");
        this.dimensions     = dimensions;
        this.numBuckets     = numBuckets;
        this.projectionVector = randomUnitVector(dimensions);
        this.criticalAngles   = new double[numBuckets - 1];
    }

    /**
     * Projects a point onto the internal random hyperplane.
     */
    public double project(double[] point) {
        if (point == null || point.length != dimensions)
            throw new IllegalArgumentException("Invalid point dimension");
        double dot = 0;
        for (int i = 0; i < dimensions; i++) dot += point[i] * projectionVector[i];
        return dot;
    }

    /**
     * Returns bucket ID in [1..numBuckets] by comparing to cut-points.
     */
    public int getBucketId(double[] point) {
        double proj = project(point);
        for (int i = 0; i < criticalAngles.length; i++) {
            if (proj <= criticalAngles[i]) return i + 1;
        }
        return numBuckets;
    }

    /**
     * Recomputes quantile cut-points from dataset.
     */
    public void updateCriticalValues(List<double[]> data) {
        if (data == null || data.isEmpty())
            throw new IllegalArgumentException("Data must be non-empty");
        List<Double> proj = new ArrayList<>(data.size());
        for (double[] v : data) proj.add(project(v));
        Collections.sort(proj);
        for (int k = 1; k < numBuckets; k++) {
            int idx = (int)Math.floor(k * proj.size() / (double) numBuckets);
            criticalAngles[k - 1] = proj.get(Math.min(idx, proj.size() - 1));
        }
        logger.debug("Updated {} cut-points", criticalAngles.length);
    }

    /**
     * Returns neighboring buckets in +/- range.
     */
    public List<Integer> expandBuckets(int mainBucket, int range) {
        if (mainBucket < 1 || mainBucket > numBuckets)
            throw new IllegalArgumentException("mainBucket out of range");
        List<Integer> out = new ArrayList<>();
        for (int d = -range; d <= range; d++) {
            int b = mainBucket + d;
            if (b >= 1 && b <= numBuckets) out.add(b);
        }
        return out;
    }

    private static double[] randomUnitVector(int d) {
        SecureRandom rng = new SecureRandom();
        double[] v = new double[d];
        double norm = 0;
        for (int i = 0; i < d; i++) {
            v[i] = rng.nextGaussian();
            norm += v[i] * v[i];
        }
        norm = Math.sqrt(norm);
        for (int i = 0; i < d; i++) v[i] /= norm;
        return v;
    }
}
