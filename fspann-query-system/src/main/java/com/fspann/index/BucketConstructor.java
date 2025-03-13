package com.fspann.index;

import com.fspann.encryption.EncryptionUtils;
import javax.crypto.SecretKey;
import java.util.ArrayList;
import java.util.List;

/**
 * Constructs and manages buckets of high-dimensional data points for LSH indexing.
 * Supports encryption, dynamic updates, and forward security.
 */
public class BucketConstructor {

    private static final double FAKE_POINT_MARKER = -1.0; // Marker to identify fake points

    /**
     * Merge sorted data points into buckets of at most maxBucketSize.
     * @param sortedPoints List of data points already sorted by their LSH bucket ID.
     * @param maxBucketSize Maximum number of points per bucket.
     * @param key SecretKey for encryption (null if no encryption needed).
     * @return List of encrypted buckets, where each bucket is a List of encrypted byte[].
     */
    public static List<List<byte[]>> greedyMerge(List<double[]> sortedPoints, int maxBucketSize, SecretKey key) throws Exception {
        List<List<byte[]>> buckets = new ArrayList<>();
        List<byte[]> currentBucket = new ArrayList<>();

        for (double[] point : sortedPoints) {
            byte[] encryptedPoint = (key != null) ? EncryptionUtils.encryptVector(point, key) : doubleToByteArray(point);
            currentBucket.add(encryptedPoint);
            if (currentBucket.size() >= maxBucketSize) {
                buckets.add(new ArrayList<>(currentBucket));
                currentBucket.clear();
            }
        }
        if (!currentBucket.isEmpty()) {
            buckets.add(currentBucket);
        }
        return buckets;
    }

    /**
     * Pad each bucket with fake points so that all buckets have the same size.
     * @param buckets List of encrypted buckets.
     * @param targetSize Target uniform size.
     * @param key SecretKey for encryption (null if no encryption needed).
     * @return New list of encrypted buckets with uniform size.
     */
    public static List<List<byte[]>> applyFakeAddition(List<List<byte[]>> buckets, int targetSize, SecretKey key) throws Exception {
        List<List<byte[]>> uniformBuckets = new ArrayList<>();
        for (List<byte[]> bucket : buckets) {
            List<byte[]> newBucket = new ArrayList<>();
            // Decrypt to add fake points if needed, then re-encrypt
            for (byte[] encryptedPoint : bucket) {
                newBucket.add(encryptedPoint);
            }
            int numFake = targetSize - bucket.size();
            for (int i = 0; i < numFake; i++) {
                double[] fakePoint = generateFakePoint(bucket.get(0).length / Double.BYTES); // Assume byte[] contains doubles
                byte[] encryptedFake = (key != null) ? EncryptionUtils.encryptVector(fakePoint, key) : doubleToByteArray(fakePoint);
                newBucket.add(encryptedFake);
            }
            uniformBuckets.add(newBucket);
        }
        return uniformBuckets;
    }

    /**
     * Reconstruct buckets with a new key for forward security.
     * @param buckets List of encrypted buckets.
     * @param oldKey Previous key used for encryption.
     * @param newKey New key for re-encryption.
     * @return List of re-encrypted buckets.
     */
    public static List<List<byte[]>> reEncryptBuckets(List<List<byte[]>> buckets, SecretKey oldKey, SecretKey newKey) throws Exception {
        List<List<byte[]>> reEncryptedBuckets = new ArrayList<>();
        for (List<byte[]> bucket : buckets) {
            List<byte[]> newBucket = new ArrayList<>();
            for (byte[] encryptedPoint : bucket) {
                byte[] decryptedPoint = EncryptionUtils.decryptVector(encryptedPoint, oldKey);
                byte[] reEncryptedPoint = EncryptionUtils.encryptVector(decryptedPoint, newKey);
                newBucket.add(reEncryptedPoint);
            }
            reEncryptedBuckets.add(newBucket);
        }
        return reEncryptedBuckets;
    }

    /**
     * Generate a fake point with a marker to distinguish from real points.
     * @param dimension Number of dimensions.
     * @return Fake point array.
     */
    private static double[] generateFakePoint(int dimension) {
        double[] fake = new double[dimension];
        for (int i = 0; i < dimension; i++) {
            fake[i] = FAKE_POINT_MARKER + Math.random(); // Randomized dummy value with marker
        }
        return fake;
    }

    /**
     * Convert double[] to byte[] for cases where encryption is not needed.
     */
    private static byte[] doubleToByteArray(double[] vector) {
        ByteBuffer buffer = ByteBuffer.allocate(vector.length * Double.BYTES);
        for (double value : vector) {
            buffer.putDouble(value);
        }
        return buffer.array();
    }

    /**
     * Convert byte[] back to double[] (for testing or unencrypted cases).
     */
    public static double[] byteToDoubleArray(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        double[] vector = new double[bytes.length / Double.BYTES];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = buffer.getDouble();
        }
        return vector;
    }
}