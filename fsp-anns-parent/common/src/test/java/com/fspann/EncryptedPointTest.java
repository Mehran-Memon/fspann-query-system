package com.fspann;

import com.fspann.common.EncryptedPoint;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.util.List;
import java.util.Arrays;

/**
 * Comprehensive unit tests for EncryptedPoint.
 * Tests all constructors, getters, serialization, and equality.
 *
 * Run with: mvn test -Dtest=EncryptedPointTest
 */
@DisplayName("EncryptedPoint Unit Tests")
public class EncryptedPointTest {

    private EncryptedPoint point9;
    private EncryptedPoint point7;

    private static final String ID = "vector-001";
    private static final int VERSION = 1;
    private static final int DIMENSION = 128;
    private static final int SHARD_ID = 3;
    private static final int KEY_VERSION = 1;

    @BeforeEach
    public void setUp() {
        // 9-parameter constructor
        point9 = new EncryptedPoint(
                ID,
                VERSION,
                new byte[]{1, 2, 3, 4, 5, 6, 7, 8},
                new byte[]{10, 20, 30, 40, 50},
                KEY_VERSION,
                DIMENSION,
                SHARD_ID,
                List.of(1, 2, 5, 10),
                List.of()
        );

        // 7-parameter convenience constructor
        point7 = new EncryptedPoint(
                "vector-002",
                VERSION,
                new byte[]{1, 2, 3, 4, 5, 6, 7, 8},
                new byte[]{10, 20, 30, 40, 50},
                KEY_VERSION,
                DIMENSION,
                List.of()
        );
    }

    // ============ BASIC GETTER TESTS ============

    @Test
    @DisplayName("Test getId returns correct ID")
    public void testGetId() {
        assertEquals(ID, point9.getId());
        assertEquals("vector-002", point7.getId());
    }

    @Test
    @DisplayName("Test getVersion returns correct version")
    public void testGetVersion() {
        assertEquals(VERSION, point9.getVersion());
        assertEquals(1, point7.getVersion());
    }

    @Test
    @DisplayName("Test getKeyVersion returns correct key version")
    public void testGetKeyVersion() {
        assertEquals(KEY_VERSION, point9.getKeyVersion());
        assertEquals(1, point7.getKeyVersion());
    }

    @Test
    @DisplayName("Test getDimension returns correct dimension")
    public void testGetDimension() {
        assertEquals(DIMENSION, point9.getDimension());
        assertEquals(DIMENSION, point7.getDimension());
    }

    @Test
    @DisplayName("Test getVectorLength is alias for getDimension")
    public void testGetVectorLength() {
        assertEquals(DIMENSION, point9.getVectorLength());
        assertEquals(point9.getDimension(), point9.getVectorLength());
    }

    @Test
    @DisplayName("Test getShardId returns correct shard")
    public void testGetShardId() {
        assertEquals(SHARD_ID, point9.getShardId());
        // 7-param constructor should default to 0
        assertEquals(0, point7.getShardId());
    }

    // ============ BUCKET TESTS ============

    @Test
    @DisplayName("Test getBuckets returns non-null list")
    public void testGetBucketsNonNull() {
        assertNotNull(point9.getBuckets());
        assertNotNull(point7.getBuckets());
    }

    @Test
    @DisplayName("Test getBuckets returns correct values")
    public void testGetBucketsValues() {
        List<Integer> buckets = point9.getBuckets();
        assertEquals(4, buckets.size());
        assertTrue(buckets.contains(1));
        assertTrue(buckets.contains(2));
        assertTrue(buckets.contains(5));
        assertTrue(buckets.contains(10));
    }

    @Test
    @DisplayName("Test getBuckets empty list for 7-param constructor")
    public void testGetBucketsEmpty() {
        List<Integer> buckets = point7.getBuckets();
        assertNotNull(buckets);
        assertTrue(buckets.isEmpty());
    }

    // ============ CIPHERTEXT TESTS ============

    @Test
    @DisplayName("Test getIv returns non-null iv")
    public void testGetIvNonNull() {
        assertNotNull(point9.getIv());
        assertNotNull(point7.getIv());
    }

    @Test
    @DisplayName("Test getIv returns correct length")
    public void testGetIvLength() {
        assertEquals(8, point9.getIv().length);
        assertEquals(8, point7.getIv().length);
    }

    @Test
    @DisplayName("Test getCiphertext returns non-null ciphertext")
    public void testGetCiphertextNonNull() {
        assertNotNull(point9.getCiphertext());
        assertNotNull(point7.getCiphertext());
    }

    @Test
    @DisplayName("Test getCiphertext returns correct length")
    public void testGetCiphertextLength() {
        assertEquals(5, point9.getCiphertext().length);
        assertEquals(5, point7.getCiphertext().length);
    }

    // ============ SERIALIZATION TESTS ============

    @Test
    @DisplayName("Test EncryptedPoint implements Serializable")
    public void testIsSerializable() {
        assertTrue(point9 instanceof Serializable);
        assertTrue(point7 instanceof Serializable);
    }

    @Test
    @DisplayName("Test serialization roundtrip preserves all fields")
    public void testSerializationRoundtrip() throws IOException, ClassNotFoundException {
        // Serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(point9);
        oos.close();

        // Deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        EncryptedPoint deserialized = (EncryptedPoint) ois.readObject();
        ois.close();

        // Verify all fields
        assertEquals(point9.getId(), deserialized.getId());
        assertEquals(point9.getVersion(), deserialized.getVersion());
        assertEquals(point9.getKeyVersion(), deserialized.getKeyVersion());
        assertEquals(point9.getDimension(), deserialized.getDimension());
        assertEquals(point9.getShardId(), deserialized.getShardId());
        assertEquals(point9.getBuckets().size(), deserialized.getBuckets().size());
    }

    @Test
    @DisplayName("Test serialized data can be deserialized multiple times")
    public void testMultipleDeserializations() throws IOException, ClassNotFoundException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(point9);
        oos.close();
        byte[] serialized = baos.toByteArray();

        // Deserialize twice
        EncryptedPoint des1 = deserialize(serialized);
        EncryptedPoint des2 = deserialize(serialized);

        assertEquals(des1.getId(), des2.getId());
        assertEquals(des1.getDimension(), des2.getDimension());
    }

    // ============ EQUALITY TESTS ============

    @Test
    @DisplayName("Test equals returns true for identical points")
    public void testEqualsIdentical() {
        EncryptedPoint point2 = new EncryptedPoint(
                ID,
                VERSION,
                new byte[]{1, 2, 3, 4, 5, 6, 7, 8},
                new byte[]{10, 20, 30, 40, 50},
                KEY_VERSION,
                DIMENSION,
                SHARD_ID,
                List.of(1, 2, 5, 10),
                List.of()
        );

        assertEquals(point9, point2);
    }

    @Test
    @DisplayName("Test equals returns false for different IDs")
    public void testNotEqualsDifferentId() {
        EncryptedPoint point2 = new EncryptedPoint(
                "vector-999",  // Different ID
                VERSION,
                new byte[]{1, 2, 3, 4, 5, 6, 7, 8},
                new byte[]{10, 20, 30, 40, 50},
                KEY_VERSION,
                DIMENSION,
                SHARD_ID,
                List.of(1, 2, 5, 10),
                List.of()
        );

        assertNotEquals(point9, point2);
    }

    @Test
    @DisplayName("Test equals returns false for different dimensions")
    public void testNotEqualsDifferentDimension() {
        EncryptedPoint point2 = new EncryptedPoint(
                ID,
                VERSION,
                new byte[]{1, 2, 3, 4, 5, 6, 7, 8},
                new byte[]{10, 20, 30, 40, 50},
                KEY_VERSION,
                256,  // Different dimension
                SHARD_ID,
                List.of(1, 2, 5, 10),
                List.of()
        );

        assertNotEquals(point9, point2);
    }

    @Test
    @DisplayName("Test hashCode consistent with equals")
    public void testHashCodeConsistency() {
        EncryptedPoint point2 = new EncryptedPoint(
                ID,
                VERSION,
                new byte[]{1, 2, 3, 4, 5, 6, 7, 8},
                new byte[]{10, 20, 30, 40, 50},
                KEY_VERSION,
                DIMENSION,
                SHARD_ID,
                List.of(1, 2, 5, 10),
                List.of()
        );

        if (point9.equals(point2)) {
            assertEquals(point9.hashCode(), point2.hashCode());
        }
    }

    // ============ EDGE CASE TESTS ============

    @Test
    @DisplayName("Test constructor with empty IV")
    public void testEmptyIv() {
        EncryptedPoint point = new EncryptedPoint(
                "test",
                1,
                new byte[]{},
                new byte[]{10, 20},
                1,
                128,
                0,
                List.of(),
                List.of()
        );

        assertNotNull(point);
        assertEquals(0, point.getIv().length);
    }

    @Test
    @DisplayName("Test constructor with large buckets list")
    public void testLargeBucketsList() {
        List<Integer> largeBuckets = Arrays.asList(
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20
        );

        EncryptedPoint point = new EncryptedPoint(
                "test",
                1,
                new byte[]{1, 2, 3, 4, 5, 6, 7, 8},
                new byte[]{10, 20},
                1,
                128,
                0,
                largeBuckets,
                List.of()
        );

        assertEquals(20, point.getBuckets().size());
    }

    @Test
    @DisplayName("Test constructor with different dimension values")
    public void testVariousDimensions() {
        int[] dimensions = {1, 64, 128, 256, 512, 1000, 10000};

        for (int dim : dimensions) {
            EncryptedPoint point = new EncryptedPoint(
                    "test-" + dim,
                    1,
                    new byte[]{1, 2, 3, 4, 5, 6, 7, 8},
                    new byte[]{10, 20},
                    1,
                    dim,
                    0,
                    List.of(),
                    List.of()
            );

            assertEquals(dim, point.getDimension());
            assertEquals(dim, point.getVectorLength());
        }
    }

    @Test
    @DisplayName("Test constructor with different shard IDs")
    public void testVariousShardIds() {
        int[] shardIds = {0, 1, 32, 64, 128, 256, 512, 1024, 2048};

        for (int shardId : shardIds) {
            EncryptedPoint point = new EncryptedPoint(
                    "test-" + shardId,
                    1,
                    new byte[]{1, 2, 3, 4, 5, 6, 7, 8},
                    new byte[]{10, 20},
                    1,
                    128,
                    shardId,
                    List.of(),
                    List.of()
            );

            assertEquals(shardId, point.getShardId());
        }
    }

    @Test
    @DisplayName("Test constructor with different key versions")
    public void testVariousKeyVersions() {
        int[] versions = {1, 2, 3, 10, 100, 1000};

        for (int version : versions) {
            EncryptedPoint point = new EncryptedPoint(
                    "test-v" + version,
                    version,
                    new byte[]{1, 2, 3, 4, 5, 6, 7, 8},
                    new byte[]{10, 20},
                    version,
                    128,
                    0,
                    List.of(),
                    List.of()
            );

            assertEquals(version, point.getVersion());
            assertEquals(version, point.getKeyVersion());
        }
    }

    // ============ INTEGRATION TESTS ============

    @Test
    @DisplayName("Test all getters together")
    public void testAllGettersTogether() {
        assertEquals(ID, point9.getId());
        assertEquals(VERSION, point9.getVersion());
        assertEquals(KEY_VERSION, point9.getKeyVersion());
        assertEquals(DIMENSION, point9.getDimension());
        assertEquals(DIMENSION, point9.getVectorLength());
        assertEquals(SHARD_ID, point9.getShardId());
        assertNotNull(point9.getIv());
        assertNotNull(point9.getCiphertext());
        assertNotNull(point9.getBuckets());
        assertEquals(4, point9.getBuckets().size());
    }

    @Test
    @DisplayName("Test 7-parameter constructor defaults")
    public void test7ParameterDefaults() {
        assertEquals(0, point7.getShardId());  // Should default to 0
        assertTrue(point7.getBuckets().isEmpty());  // Should be empty
    }

    // ============ HELPER METHODS ============

    private EncryptedPoint deserialize(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        ObjectInputStream ois = new ObjectInputStream(bais);
        EncryptedPoint point = (EncryptedPoint) ois.readObject();
        ois.close();
        return point;
    }
}