package com.fspann.index;

import com.fspann.ForwardSecureANNSystem;
import com.fspann.encryption.EncryptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BucketConstructor {

    private static final double FAKE_POINT_MARKER = -1.0;  // Marker for fake points
    private static final Logger logger = LoggerFactory.getLogger(BucketConstructor.class);

    /**
     * Perform greedy merging of sorted points into buckets, with encryption applied.
     * @param sortedPoints Sorted list of points.
     * @param maxBucketSize Maximum allowed size for a bucket.
     * @param key SecretKey for encryption.
     * @return List of buckets containing encrypted points.
     * @throws Exception if encryption fails.
     */
// Log bucket ID and LSH values during greedy merging
    public static List<List<byte[]>> greedyMerge(List<double[]> sortedPoints, int maxBucketSize, SecretKey key) throws Exception {
        List<List<byte[]>> buckets = new ArrayList<>();
        List<byte[]> currentBucket = new ArrayList<>();

        // Process each point
        for (double[] point : sortedPoints) {
            // Encrypt the point
            byte[] encryptedPoint = (key != null) ? EncryptionUtils.encryptVector(point, key) : doubleToByteArray(point);
            currentBucket.add(encryptedPoint);

            // Log LSH value and bucket size
            logger.info("Point: " + Arrays.toString(point) + " -> Encrypted: " + Arrays.toString(encryptedPoint));

            // If bucket size exceeds limit, add to final buckets and reset current bucket
            if (currentBucket.size() >= maxBucketSize) {
                logger.info("Bucket added with " + currentBucket.size() + " points.");
                buckets.add(new ArrayList<>(currentBucket));
                currentBucket.clear();
            }
        }

        // Add any remaining points
        if (!currentBucket.isEmpty()) {
            logger.info("Final bucket added with " + currentBucket.size() + " points.");
            buckets.add(currentBucket);
        }

        return buckets;
    }

    /**
     * Apply fake point addition to each bucket, ensuring each bucket has the target size.
     * @param buckets The buckets to apply fake points to.
     * @param targetSize The target size for each bucket.
     * @param key The SecretKey for encryption.
     * @param dimension The dimensionality of the points.
     * @return A list of buckets with fake points added.
     * @throws Exception if encryption fails.
     */
    public static List<List<byte[]>> applyFakeAddition(List<List<byte[]>> buckets, int targetSize, SecretKey key, int dimension) throws Exception {
        List<List<byte[]>> uniformBuckets = new ArrayList<>();
        int totalFakePointsAdded = 0;

        // Process each bucket
        for (List<byte[]> bucket : buckets) {
            List<byte[]> newBucket = new ArrayList<>(bucket);
            int numFake = targetSize - bucket.size();  // Calculate how many fake points to add

            // Log before adding fake points
            if (numFake > 0) {
                logger.info("Adding " + numFake + " fake points to bucket.");
            }

            for (int i = 0; i < numFake; i++) {
                // Generate and encrypt a fake point
                double[] fakePoint = generateFakePoint(dimension);
                byte[] encryptedFake = (key != null) ? EncryptionUtils.encryptVector(fakePoint, key) : doubleToByteArray(fakePoint);
                newBucket.add(encryptedFake);
                totalFakePointsAdded++;
            }

            uniformBuckets.add(newBucket);
        }

        logger.info("Total fake points added: " + totalFakePointsAdded);
        return uniformBuckets;
    }

    /**
     * Re-encrypt all buckets using the new key.
     * @param buckets The buckets to re-encrypt.
     * @param oldKey The old key used for decryption.
     * @param newKey The new key used for encryption.
     * @return A new list of re-encrypted buckets.
     * @throws Exception if decryption or encryption fails.
     */
    public static List<List<byte[]>> reEncryptBuckets(List<List<byte[]>> buckets, SecretKey oldKey, SecretKey newKey) throws Exception {
        List<List<byte[]>> reEncryptedBuckets = new ArrayList<>();

        // Re-encrypt each bucket
        for (List<byte[]> bucket : buckets) {
            List<byte[]> newBucket = new ArrayList<>();
            for (byte[] encryptedPoint : bucket) {
                double[] decryptedPoint = EncryptionUtils.decryptVector(encryptedPoint, oldKey);  // Decrypt point
                byte[] reEncryptedPoint = EncryptionUtils.encryptVector(decryptedPoint, newKey);  // Re-encrypt point
                newBucket.add(reEncryptedPoint);
            }
            reEncryptedBuckets.add(newBucket);
        }

        return reEncryptedBuckets;
    }

    /**
     * Generate a fake point with random values.
     * @param dimension The number of dimensions for the fake point.
     * @return The generated fake point.
     */
    private static double[] generateFakePoint(int dimension) {
        double[] fake = new double[dimension];
        for (int i = 0; i < dimension; i++) {
            fake[i] = FAKE_POINT_MARKER + Math.random();  // Assign fake values with a marker
        }
        return fake;
    }

    /**
     * Convert a double[] vector to a byte[] for storage or transmission.
     * @param vector The vector to convert.
     * @return The byte[] representation of the vector.
     */
    private static byte[] doubleToByteArray(double[] vector) {
        ByteBuffer buffer = ByteBuffer.allocate(vector.length * Double.BYTES);
        for (double value : vector) {
            buffer.putDouble(value);  // Put each value as a double
        }
        return buffer.array();
    }

    /**
     * Convert a byte[] back into a double[] vector.
     * @param bytes The byte array to convert.
     * @return The double[] representation of the byte array.
     */
    public static double[] byteToDoubleArray(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        double[] vector = new double[bytes.length / Double.BYTES];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = buffer.getDouble();  // Get each double from the buffer
        }
        return vector;
    }
}
