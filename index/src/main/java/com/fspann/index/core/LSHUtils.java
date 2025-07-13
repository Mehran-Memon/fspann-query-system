package com.fspann.index.core;

import com.fspann.common.EncryptedPoint;
import com.fspann.crypto.CryptoService;

import javax.crypto.SecretKey;
import java.util.*;

/**
 * Utility methods for LSH operations in the encrypted domain.
 * Supports projection computation and quantile-based bucket boundary estimation.
 * These methods assist in secure re-indexing and adaptive partitioning.
 */
public class LSHUtils {

    /**
     * Decrypts each EncryptedPoint using the provided CryptoService and SecretKey,
     * then computes the projection of the decrypted vector using the EvenLSH instance.
     *
     * @param points  list of encrypted points
     * @param lsh     EvenLSH instance used for projection
     * @param crypto  CryptoService for decryption
     * @param key     SecretKey used for decryption
     * @return        list of projection values for each point
     */
    public static List<Double> computeProjections(
            List<EncryptedPoint> points,
            EvenLSH lsh,
            CryptoService crypto,
            SecretKey key) {

        if (points == null || lsh == null || crypto == null || key == null) {
            throw new IllegalArgumentException("Arguments must not be null");
        }

        List<Double> projections = new ArrayList<>(points.size());
        for (EncryptedPoint pt : points) {
            double[] vector = crypto.decryptFromPoint(pt, key);
            projections.add(lsh.project(vector));
        }
        return projections;
    }

    /**
     * Computes quantile-based boundaries to divide data into nearly equal-sized buckets.
     * Can be used for dynamic, data-driven partitioning of projection values.
     *
     * @param data        list of numeric projections (unsorted or sorted)
     * @param numBuckets  desired number of buckets
     * @return            array of bucket boundary values (length = numBuckets)
     */
    public static double[] quantiles(List<Double> data, int numBuckets) {
        if (data == null || data.isEmpty()) {
            throw new IllegalArgumentException("Data list must not be null or empty");
        }
        if (numBuckets <= 0) {
            throw new IllegalArgumentException("numBuckets must be positive");
        }

        List<Double> sorted = new ArrayList<>(data);
        Collections.sort(sorted);

        int n = sorted.size();
        double[] boundaries = new double[numBuckets];
        for (int i = 1; i <= numBuckets; i++) {
            int idx = (int) Math.floor(i * n / (double) (numBuckets + 1));
            boundaries[i - 1] = sorted.get(Math.min(idx, n - 1));
        }
        return boundaries;
    }

    private LSHUtils() {
        // Utility class should not be instantiated
    }
}
