package com.index;

import com.fspann.index.lsh.RandomProjectionLSH;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for RandomProjectionLSH implementation.
 *
 * Tests:
 *   - Hash function properties (determinism, range)
 *   - Collision probability estimation
 *   - Serialization/deserialization
 *   - Batch hashing
 *
 * @author FSP-ANNS Project
 * @version 1.0
 */
@DisplayName("LSHHashFamily Tests")
public class LSHHashFamilyTest {

    private RandomProjectionLSH lsh;
    private int dimension = 128;
    private int numTables = 5;
    private int numFunctions = 3;
    private int numBuckets = 100;

    @BeforeEach
    void setUp() {
        lsh = new RandomProjectionLSH();
        lsh.init(dimension, numTables, numFunctions, numBuckets);
    }

    /**
     * Test that hash function is deterministic.
     * Same vector + same table/function should always produce same hash.
     */
    @Test
    @DisplayName("Hash should be deterministic")
    void testHashDeterminism() {
        double[] vector = new double[dimension];
        for (int i = 0; i < dimension; i++) {
            vector[i] = Math.random();
        }

        int hash1 = lsh.hash(vector, 0, 0);
        int hash2 = lsh.hash(vector, 0, 0);
        int hash3 = lsh.hash(vector, 0, 0);

        assertEquals(hash1, hash2, "Hash should be deterministic");
        assertEquals(hash2, hash3, "Hash should be deterministic");
    }

    /**
     * Test that hash values are in valid range [0, numBuckets).
     */
    @Test
    @DisplayName("Hash values should be in valid range")
    void testHashRange() {
        double[] vector = new double[dimension];
        for (int i = 0; i < dimension; i++) {
            vector[i] = Math.random();
        }

        for (int t = 0; t < numTables; t++) {
            for (int k = 0; k < numFunctions; k++) {
                int hash = lsh.hash(vector, t, k);
                assertTrue(hash >= 0 && hash < numBuckets,
                        "Hash out of range: " + hash);
            }
        }
    }

    /**
     * Test that different tables produce different hashes.
     */
    @Test
    @DisplayName("Different tables should produce different hashes")
    void testTableIndependence() {
        double[] vector = new double[dimension];
        for (int i = 0; i < dimension; i++) {
            vector[i] = Math.random();
        }

        int hash1 = lsh.hash(vector, 0, 0);
        int hash2 = lsh.hash(vector, 1, 0);
        int hash3 = lsh.hash(vector, 2, 0);

        // Hashes should be different (with high probability)
        // Note: This is probabilistic, small chance of false positive
        int diffCount = 0;
        if (hash1 != hash2) diffCount++;
        if (hash2 != hash3) diffCount++;
        if (hash1 != hash3) diffCount++;

        assertTrue(diffCount >= 2,
                "Tables should produce mostly different hashes");
    }

    /**
     * Test that different functions produce different hashes.
     */
    @Test
    @DisplayName("Different functions should produce different hashes")
    void testFunctionIndependence() {
        double[] vector = new double[dimension];
        for (int i = 0; i < dimension; i++) {
            vector[i] = Math.random();
        }

        int hash1 = lsh.hash(vector, 0, 0);
        int hash2 = lsh.hash(vector, 0, 1);
        int hash3 = lsh.hash(vector, 0, 2);

        // Hashes should be different (with high probability)
        int diffCount = 0;
        if (hash1 != hash2) diffCount++;
        if (hash2 != hash3) diffCount++;
        if (hash1 != hash3) diffCount++;

        assertTrue(diffCount >= 2,
                "Functions should produce mostly different hashes");
    }

    /**
     * Test batch hashing.
     */
    @Test
    @DisplayName("Batch hashing should match individual calls")
    void testBatchHashing() {
        int numVectors = 10;
        double[][] vectors = new double[numVectors][dimension];
        for (int i = 0; i < numVectors; i++) {
            for (int d = 0; d < dimension; d++) {
                vectors[i][d] = Math.random();
            }
        }

        int[][] batchHashes = lsh.hashBatch(vectors, 0);

        assertEquals(numVectors, batchHashes.length,
                "Batch size mismatch");
        assertEquals(numFunctions, batchHashes[0].length,
                "Hash functions mismatch");

        // Verify batch matches individual calls
        for (int i = 0; i < numVectors; i++) {
            for (int k = 0; k < numFunctions; k++) {
                int batchHash = batchHashes[i][k];
                int singleHash = lsh.hash(vectors[i], 0, k);
                assertEquals(batchHash, singleHash,
                        "Batch hash mismatch at [" + i + "][" + k + "]");
            }
        }
    }

    /**
     * Test collision probability is in valid range [0, 1].
     */
    @Test
    @DisplayName("Collision probability should be in range [0, 1]")
    void testCollisionProbabilityRange() {
        for (double distance = 0; distance <= 100; distance += 10) {
            for (int t = 0; t < numTables; t++) {
                double prob = lsh.collisionProbability(distance, t);
                assertTrue(prob >= 0 && prob <= 1.0,
                        "Collision prob out of range [0,1]: " + prob);
            }
        }
    }

    /**
     * Test that collision probability decreases with distance.
     */
    @Test
    @DisplayName("Collision probability should decrease with distance")
    void testCollisionProbabilityMonotonicity() {
        int tableId = 0;

        double prob1 = lsh.collisionProbability(1.0, tableId);
        double prob2 = lsh.collisionProbability(2.0, tableId);
        double prob3 = lsh.collisionProbability(3.0, tableId);

        assertTrue(prob1 >= prob2,
                "Collision prob should decrease with distance");
        assertTrue(prob2 >= prob3,
                "Collision prob should decrease with distance");
    }

    /**
     * Test that collision probability is 1.0 for distance 0.
     */
    @Test
    @DisplayName("Collision probability at zero distance should be ~1.0")
    void testCollisionProbabilityAtZeroDistance() {
        double prob = lsh.collisionProbability(0.0, 0);
        assertEquals(1.0, prob, 0.01,
                "Collision prob should be 1.0 for distance 0");
    }

    /**
     * Test serialization and deserialization.
     */
    @Test
    @DisplayName("Serialization should preserve hash behavior")
    void testSerializationDeserialization() {
        // Serialize
        byte[] serialized = lsh.serialize();
        assertNotNull(serialized,
                "Serialized data should not be null");
        assertTrue(serialized.length > 0,
                "Serialized data should not be empty");

        // Create new LSH and deserialize
        RandomProjectionLSH lsh2 = new RandomProjectionLSH();
        lsh2.deserialize(serialized);

        // Test that both produce same hashes
        double[] vector = new double[dimension];
        for (int i = 0; i < dimension; i++) {
            vector[i] = Math.random();
        }

        for (int t = 0; t < numTables; t++) {
            for (int k = 0; k < numFunctions; k++) {
                int hash1 = lsh.hash(vector, t, k);
                int hash2 = lsh2.hash(vector, t, k);
                assertEquals(hash1, hash2,
                        "Hash mismatch after deserialization at [" + t + "][" + k + "]");
            }
        }
    }

    /**
     * Test bucket width adjustment.
     */
    @Test
    @DisplayName("Bucket width adjustment should work correctly")
    void testBucketWidthAdjustment() {
        // Get initial width
        double initialWidth = lsh.getBucketWidth(0);

        // Widen buckets
        lsh.adjustBucketWidth(0, 0.95);
        double widenedWidth = lsh.getBucketWidth(0);

        assertTrue(widenedWidth < initialWidth,
                "Widened width should be less than initial (wider = smaller width value)");

        // Narrow buckets
        lsh.adjustBucketWidth(0, 1.05);
        double narrowedWidth = lsh.getBucketWidth(0);

        assertTrue(narrowedWidth > widenedWidth,
                "Narrowed width should be greater than widened");
    }

    /**
     * Test that vectors with small distance have higher collision probability.
     */
    @Test
    @DisplayName("Similar vectors should have high collision probability")
    void testCollisionProbabilityForSimilarVectors() {
        double[] v1 = new double[dimension];
        double[] v2 = new double[dimension];

        // Fill with random values
        for (int i = 0; i < dimension; i++) {
            v1[i] = Math.random();
        }

        // v2 very close to v1
        for (int i = 0; i < dimension; i++) {
            v2[i] = v1[i] + 0.001 * Math.random();
        }

        // Compute distance
        double distance = 0;
        for (int i = 0; i < dimension; i++) {
            double diff = v1[i] - v2[i];
            distance += diff * diff;
        }
        distance = Math.sqrt(distance);

        // Similar vectors should have moderate collision probability
        double prob = lsh.collisionProbability(distance, 0);
        assertTrue(prob > 0.3,
                "Similar vectors should have high collision probability");
    }

    /**
     * Test error handling for invalid arguments.
     */
    @Test
    @DisplayName("Invalid arguments should throw exceptions")
    void testErrorHandling() {
        double[] vector = new double[dimension];

        // Wrong dimension
        assertThrows(IllegalArgumentException.class, () -> {
            double[] wrongDimVector = new double[64];
            lsh.hash(wrongDimVector, 0, 0);
        }, "Should throw exception for wrong dimension");

        // Invalid table ID
        assertThrows(IllegalArgumentException.class, () -> {
            lsh.hash(vector, -1, 0);
        }, "Should throw exception for invalid table ID");

        // Invalid function ID
        assertThrows(IllegalArgumentException.class, () -> {
            lsh.hash(vector, 0, -1);
        }, "Should throw exception for invalid function ID");
    }

    /**
     * Test configuration string.
     */
    @Test
    @DisplayName("Configuration string should contain settings")
    void testGetConfiguration() {
        String config = lsh.getConfiguration();
        assertNotNull(config,
                "Configuration should not be null");
        assertTrue(config.contains("128"),
                "Configuration should contain dimension");
        assertTrue(config.contains("5"),
                "Configuration should contain tables");
    }
}