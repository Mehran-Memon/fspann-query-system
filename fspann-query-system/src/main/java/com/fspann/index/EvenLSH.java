package com.fspann.index;

import com.fspann.encryption.EncryptionUtils;
import javax.crypto.SecretKey;
import java.security.SecureRandom;

/**
 * EvenLSH implements a Locality-Sensitive Hashing (LSH) variant that ensures even bucket division.
 * Projects points onto a random unit vector and maps them to buckets using critical values.
 */
public class EvenLSH {

    private double[] a;               // Unit vector for projection
    private double[] criticalValues;  // Critical values for even bucket division
    private final int dimensions;     // Number of dimensions

    /**
     * Constructor for EvenLSH.
     * @param a Unit vector for projection.
     * @param criticalValues Sorted array of critical values defining bucket boundaries.
     * @param dimensions Number of dimensions of the data points.
     */
    public EvenLSH(double[] a, double[] criticalValues, int dimensions) {
        this.a = a;
        this.criticalValues = criticalValues;
        this.dimensions = dimensions;
    }

    /**
     * Map a high-dimensional point to a bucket ID using plaintext data.
     * @param point The data point (array of doubles).
     * @return Bucket ID as an integer (1-indexed).
     */
    public int getBucketId(double[] point) {
        if (point.length != dimensions) {
            throw new IllegalArgumentException("Point dimension mismatch: expected " + dimensions + ", got " + point.length);
        }
        double projection = 0.0;
        for (int i = 0; i < point.length; i++) {
            projection += point[i] * a[i];
        }
        for (int i = 0; i < criticalValues.length; i++) {
            if (projection <= criticalValues[i]) {
                return i + 1; // 1-indexed bucket IDs
            }
        }
        return criticalValues.length + 1;
    }

    /**
     * Map an encrypted high-dimensional point to a bucket ID.
     * @param encryptedPoint Encrypted data point (byte[]).
     * @param key SecretKey for decryption.
     * @return Bucket ID as an integer (1-indexed).
     */
    public int getBucketId(byte[] encryptedPoint, SecretKey key) throws Exception {
        double[] point = EncryptionUtils.decryptVector(encryptedPoint, key);
        return getBucketId(point);
    }

/**
 * Update critical values based on new data distribution.
 * @param points List of points to