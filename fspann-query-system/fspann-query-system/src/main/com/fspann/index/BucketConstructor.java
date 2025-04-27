package com.fspann.index;

import com.fspann.ForwardSecureANNSystem;
import com.fspann.encryption.EncryptionUtils;
import javax.crypto.SecretKey;
import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BucketConstructor {

    private static final double FAKE_POINT_MARKER = -1.0;  // Marker for fake points
    private static final Logger logger = LoggerFactory.getLogger(BucketConstructor.class);

    // Method to perform greedy merge based on Even LSH
    public static List<List<byte[]>> greedyMerge(List<double[]> sortedPoints, int maxBucketSize, EvenLSH lsh, SecretKey key) throws Exception {
        List<List<byte[]>> buckets = new ArrayList<>();
        List<byte[]> currentBucket = new ArrayList<>();

        // Process each point and assign to buckets based on Even LSH
        for (double[] point : sortedPoints) {
            // Get bucket ID using Even LSH
            int bucketId = lsh.getBucketId(point);

            // Encrypt the point and add to current bucket
            byte[] encryptedPoint = (key != null) ? EncryptionUtils.encryptVector(point, key) : doubleToByteArray(point);
            currentBucket.add(encryptedPoint);

            // If bucket size exceeds limit, add to final buckets and reset current bucket
            if (currentBucket.size() >= maxBucketSize) {
                buckets.add(new ArrayList<>(currentBucket));
                currentBucket.clear();
            }
        }

        // Add any remaining points
        if (!currentBucket.isEmpty()) {
            buckets.add(currentBucket);
        }

        // Log bucket sizes after merge
        logBucketSizes(buckets);

        return buckets;
    }

    // Method to apply fake point insertion to ensure balanced bucket sizes
    public static List<List<byte[]>> applyFakeAddition(List<List<byte[]>> buckets, int targetSize, SecretKey key, int dimension) throws Exception {
        List<List<byte[]>> uniformBuckets = new ArrayList<>();
        int totalFakePointsAdded = 0;

        // Calculate the average bucket size
        int totalSize = 0;
        for (List<byte[]> bucket : buckets) {
            totalSize += bucket.size();
        }
        double averageSize = (double) totalSize / buckets.size();

        // Process each bucket
        for (List<byte[]> bucket : buckets) {
            List<byte[]> newBucket = new ArrayList<>(bucket);
            int bucketSize = bucket.size();
            int numFake = 0;

            // Add fake points if the bucket size is less than 90% of the target size
            if (bucketSize < targetSize * 0.9) { // Add fake points if bucket is less than 90% of target size
                numFake = targetSize - bucketSize;
            }

            // Limit fake points to prevent excessive addition
            numFake = Math.min(numFake, 10); // Adjust the threshold for fake points

            // Add fake points
            for (int i = 0; i < numFake; i++) {
                double[] fakePoint = generateFakePoint(dimension);
                byte[] encryptedFake = (key != null) ? EncryptionUtils.encryptVector(fakePoint, key) : doubleToByteArray(fakePoint);
                newBucket.add(encryptedFake);
                totalFakePointsAdded++;
            }

            uniformBuckets.add(newBucket);
        }

        // Log the total fake points added
        System.out.println("Total fake points added: " + totalFakePointsAdded);

        // Log bucket sizes after fake point insertion
        logBucketSizes(uniformBuckets);

        return uniformBuckets;
    }

    // Helper method to generate fake points
    private static double[] generateFakePoint(int dimension) {
        double[] fake = new double[dimension];
        for (int i = 0; i < dimension; i++) {
            fake[i] = FAKE_POINT_MARKER + Math.random();  // Assign fake values with a marker
        }
        return fake;
    }

    /**
     * Converts a byte array into a double array.
     * @param bytes The byte array to convert.
     * @return The converted double array.
     */
    public static double[] byteToDoubleArray(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        double[] vector = new double[bytes.length / Double.BYTES];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = buffer.getDouble();  // Extract each double from the byte buffer
        }
        return vector;
    }

    // Helper method to convert double[] to byte[]
    private static byte[] doubleToByteArray(double[] vector) {
        ByteBuffer buffer = ByteBuffer.allocate(vector.length * Double.BYTES);
        for (double value : vector) {
            buffer.putDouble(value);  // Put each value as a double
        }
        return buffer.array();
    }

    // Log bucket sizes for tracking
    private static void logBucketSizes(List<List<byte[]>> buckets) {
        for (int i = 0; i < buckets.size(); i++) {
            System.out.println("Bucket " + (i + 1) + " size: " + buckets.get(i).size());
        }
    }
}
