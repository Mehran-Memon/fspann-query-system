package com.fspann.index;

import com.fspann.encryption.EncryptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.crypto.SecretKey;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Collections;

public class LSHUtils {

    private static final Logger logger = LoggerFactory.getLogger(LSHUtils.class);

    /**
     * Compute critical values (quantiles) for even division using plaintext data.
     * @param data List of high-dimensional data points.
     * @param a Unit vector for projection.
     * @param numIntervals Number of intervals (buckets) desired.
     * @return Array of critical values.
     */
// Log the critical values computed for the bucket division
    public static double[] computeCriticalValues(List<double[]> data, double[] a, int numIntervals) {
        if (data == null || data.isEmpty()) {
            throw new IllegalArgumentException("Data list cannot be null or empty.");
        }
        if (numIntervals < 1) {
            throw new IllegalArgumentException("Number of intervals must be at least 1.");
        }

        // Calculate projections
        List<Double> projections = new ArrayList<>();
        for (double[] point : data) {
            if (point.length != a.length) {
                throw new IllegalArgumentException("Point dimension mismatch: expected " + a.length + ", got " + point.length);
            }
            double proj = 0.0;
            for (int i = 0; i < point.length; i++) {
                proj += point[i] * a[i];
            }
            projections.add(proj);
        }

        // Sort the projections
        Collections.sort(projections);
        int n = projections.size();
        double[] criticalValues = new double[numIntervals];

        // Compute the critical values (quantiles)
        for (int i = 1; i <= numIntervals; i++) {
            int index = (int) Math.floor(i * n / (double) (numIntervals + 1));
            criticalValues[i - 1] = projections.get(Math.min(index, projections.size() - 1));
        }

        // Log the critical values
        logger.info("Critical values: " + Arrays.toString(criticalValues));

        return criticalValues;
    }

    /**
     * Compute critical values (quantiles) for even division using encrypted data.
     * @param encryptedData List of encrypted high-dimensional data points.
     * @param a Unit vector for projection.
     * @param numIntervals Number of intervals (buckets) desired.
     * @param key SecretKey for decryption.
     * @return Array of critical values.
     */
    public static double[] computeCriticalValuesEncrypted(List<byte[]> encryptedData, double[] a, int numIntervals, SecretKey key) throws Exception {
        if (encryptedData == null || encryptedData.isEmpty()) {
            throw new IllegalArgumentException("Encrypted data list cannot be null or empty.");
        }

        // Decrypt data in smaller chunks for memory optimization
        List<double[]> decryptedData = new ArrayList<>();
        for (byte[] encryptedPoint : encryptedData) {
            double[] point = EncryptionUtils.decryptVector(encryptedPoint, key);
            decryptedData.add(point);
        }

        // Compute critical values for the decrypted data
        return computeCriticalValues(decryptedData, a, numIntervals);
    }

    /**
     * Generate a random unit vector for projection.
     * @param dimensions Number of dimensions.
     * @return Unit vector as double[].
     */
    public static double[] generateUnitVector(int dimensions) {
        SecureRandom random = new SecureRandom();
        double[] a = new double[dimensions];
        double norm = 0.0;
        for (int i = 0; i < dimensions; i++) {
            a[i] = random.nextGaussian();
            norm += a[i] * a[i];
        }
        norm = Math.sqrt(norm);
        for (int i = 0; i < dimensions; i++) {
            a[i] /= norm; // Normalize to unit vector
        }
        return a;
    }
}
