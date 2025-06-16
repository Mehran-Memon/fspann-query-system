package com.fspann.index.core;

import com.fspann.common.EncryptedPoint;
import com.fspann.crypto.CryptoService;

import javax.crypto.SecretKey;
import java.util.*;

/**
 * Utility methods for LSH operations in encrypted domain.
 * @deprecated This class is not currently used in the system. Consider removal if no use case is identified.
 */
public class LSHUtils {

    /**
     * Decrypts each EncryptedPoint with the provided SecretKey and computes its projection.
     *
     * @param pts   list of encrypted points
     * @param lsh   LSH instance for projections
     * @param crypto crypto service for decryption
     * @param key   secret key for decryption
     * @return list of projection values
     * @deprecated May be removed if not integrated into the workflow.
     */
    @Deprecated
    public static List<Double> computeProjections(
            List<EncryptedPoint> pts,
            EvenLSH lsh,
            CryptoService crypto,
            SecretKey key) {
        if (pts == null || lsh == null || crypto == null || key == null) {
            throw new IllegalArgumentException("Arguments must not be null");
        }
        List<Double> projections = new ArrayList<>(pts.size());
        for (EncryptedPoint ep : pts) {
            double[] vec = crypto.decryptFromPoint(ep, key);
            projections.add(lsh.project(vec));
        }
        return projections;
    }

    /**
     * Computes quantile cut-points for dynamic bucket boundaries.
     *
     * @param data       sorted or unsorted list of projection values
     * @param numBuckets number of buckets
     * @return array of quantile boundary values
     * @deprecated May be removed if not integrated into the workflow.
     */
    @Deprecated
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
        double[] q = new double[numBuckets];
        for (int i = 1; i <= numBuckets; i++) {
            int idx = (int) Math.floor(i * n / (double) (numBuckets + 1));
            q[i - 1] = sorted.get(Math.min(idx, n - 1));
        }
        return q;
    }
}