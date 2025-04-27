package com.fspann.fspann_query_system.data;

import com.fspann.index.EvenLSH;
import com.fspann.index.BucketConstructor;
import com.fspann.encryption.EncryptionUtils;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class LSHSystemTest {

    private static final int DIMENSIONS = 128; // Match SIFT dataset
    private static final int NUM_BUCKETS = 100;
    private static final int MAX_BUCKET_SIZE = 1000;
    private static final int TARGET_BUCKET_SIZE = 1000;

    private EvenLSH lsh;
    private SecretKey secretKey;

    @BeforeEach
    public void setup() throws Exception {
        // Initialize encryption key
        KeyGenerator keyGen = KeyGenerator.getInstance("AES");
        keyGen.init(128);
        secretKey = keyGen.generateKey();

        // Initialize EvenLSH
        lsh = new EvenLSH(DIMENSIONS, NUM_BUCKETS);
    }

    @Test
    public void testEvenLSHAssignsCorrectBucketId() throws Exception {
        // Create a test point with correct dimensions
        double[] testPoint = new double[DIMENSIONS];
        for (int i = 0; i < DIMENSIONS; i++) {
            testPoint[i] = Math.random();
        }

        // Encrypt and decrypt to ensure consistency
        byte[] encryptedPoint = EncryptionUtils.encryptVector(testPoint, secretKey);
        double[] decryptedPoint = EncryptionUtils.decryptVector(encryptedPoint, secretKey);

        int bucketId = lsh.getBucketId(decryptedPoint);
        assertTrue(bucketId >= 1 && bucketId <= NUM_BUCKETS, "Bucket ID should be within valid range");

        // Verify same point gets same bucket ID
        int bucketId2 = lsh.getBucketId(testPoint);
        assertEquals(bucketId, bucketId2, "Same point should get same bucket ID");
    }

    @Test
    public void testBucketConstructorGreedyMerge() throws Exception {
        // Load sample data
        List<double[]> sortedPoints = loadSampleData(1000, DIMENSIONS);

        // Perform greedy merge
        List<List<byte[]>> buckets = BucketConstructor.greedyMerge(sortedPoints, MAX_BUCKET_SIZE, lsh, secretKey);

        // Verify buckets
        assertNotNull(buckets, "Buckets should not be null");
        assertFalse(buckets.isEmpty(), "Buckets should not be empty");
        for (List<byte[]> bucket : buckets) {
            assertTrue(bucket.size() <= MAX_BUCKET_SIZE, "Bucket size should not exceed maxBucketSize");
            for (byte[] encryptedPoint : bucket) {
                double[] decryptedPoint = EncryptionUtils.decryptVector(encryptedPoint, secretKey);
                assertEquals(DIMENSIONS, decryptedPoint.length, "Decrypted point should have correct dimensions");
            }
        }
    }

    @Test
    public void testFakePointInsertion() throws Exception {
        // Create uneven buckets
        List<List<byte[]>> buckets = new ArrayList<>();
        buckets.add(createBucket(500, DIMENSIONS)); // Small bucket
        buckets.add(createBucket(800, DIMENSIONS)); // Medium bucket
        buckets.add(createBucket(1000, DIMENSIONS)); // Full bucket

        // Apply fake point insertion
        List<List<byte[]>> updatedBuckets = BucketConstructor.applyFakeAddition(buckets, TARGET_BUCKET_SIZE, secretKey, DIMENSIONS);

        // Verify results
        assertNotNull(updatedBuckets, "Updated buckets should not be null");
        assertEquals(buckets.size(), updatedBuckets.size(), "Number of buckets should remain the same");
        for (List<byte[]> bucket : updatedBuckets) {
            assertTrue(bucket.size() <= TARGET_BUCKET_SIZE + 10, "Bucket size should be close to target after fake points");
        }
    }

    @Test
    public void testCriticalValuesUpdate() throws Exception {
        // Load sample data
        List<double[]> sortedPoints = loadSampleData(1000, DIMENSIONS);

        // Update critical values
        lsh.updateCriticalValues(sortedPoints);

        // Verify
        double[] criticalValues = lsh.getCriticalValues();
        assertNotNull(criticalValues, "Critical values should not be null");
        assertEquals(NUM_BUCKETS, criticalValues.length, "Critical values should match number of buckets");
        for (int i = 1; i < criticalValues.length; i++) {
            assertTrue(criticalValues[i] >= criticalValues[i - 1], "Critical values should be non-decreasing");
        }
    }

    @Test
    public void testCriticalValuesUpdateWithEmptyData() {
        // Test edge case: empty data
        List<double[]> emptyData = new ArrayList<>();
        assertThrows(IllegalArgumentException.class, () -> lsh.updateCriticalValues(emptyData),
                "Empty data should throw IllegalArgumentException");
    }

    // Helper method to load sample data
    private List<double[]> loadSampleData(int numPoints, int dimensions) {
        List<double[]> sampleData = new ArrayList<>();
        for (int i = 0; i < numPoints; i++) {
            double[] point = new double[dimensions];
            for (int j = 0; j < dimensions; j++) {
                point[j] = Math.random();
            }
            sampleData.add(point);
        }
        return sampleData;
    }

    // Helper method to create a bucket with encrypted points
    private List<byte[]> createBucket(int size, int dimensions) throws Exception {
        List<byte[]> bucket = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            double[] point = new double[dimensions];
            for (int j = 0; j < dimensions; j++) {
                point[j] = Math.random();
            }
            bucket.add(EncryptionUtils.encryptVector(point, secretKey));
        }
        return bucket;
    }
}