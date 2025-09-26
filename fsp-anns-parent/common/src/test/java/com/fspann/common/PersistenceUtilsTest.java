package com.fspann.common;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PersistenceUtilsTest {
    @TempDir
    Path tempDir;

    @Test
    void saveAndLoadEncryptedPoint() throws IOException, ClassNotFoundException {
        Path file = tempDir.resolve("point.ser");
        List<Integer> buckets = Arrays.asList(1, 2, 3);
        EncryptedPoint original = new EncryptedPoint("vec123", 1, new byte[]{0, 1, 2}, new byte[]{3, 4, 5}, 1, 128, buckets);

        PersistenceUtils.saveObject(original, file.toString(), tempDir.toString());
        EncryptedPoint loaded = PersistenceUtils.loadObject(file.toString(), tempDir.toString(), EncryptedPoint.class);
        assertNotNull(loaded, "Loaded point should not be null");
        assertEquals(original.getId(), loaded.getId(), "Point ID mismatch");
        assertEquals(original.getShardId(), loaded.getShardId(), "Shard ID mismatch");
        assertArrayEquals(original.getIv(), loaded.getIv(), "IV mismatch");
        assertArrayEquals(original.getCiphertext(), loaded.getCiphertext(), "Ciphertext mismatch");
        assertEquals(original.getVersion(), loaded.getVersion(), "Version mismatch");
        assertEquals(original.getVectorLength(), loaded.getVectorLength(), "Vector length mismatch");
        assertEquals(original.getBuckets(), loaded.getBuckets(), "Buckets mismatch");
    }

    @Test
    void saveAndLoadEncryptedPointList() throws IOException, ClassNotFoundException {
        Path file = tempDir.resolve("points.ser");
        List<Integer> buckets = Arrays.asList(1, 2, 3);
        EncryptedPoint point1 = new EncryptedPoint("vec1", 1, new byte[]{0, 1, 2}, new byte[]{3, 4, 5}, 1, 128, buckets);
        EncryptedPoint point2 = new EncryptedPoint("vec2", 1, new byte[]{6, 7, 8}, new byte[]{9, 10, 11}, 1, 128, buckets);
        List<EncryptedPoint> original = new ArrayList<>(Arrays.asList(point1, point2));

        PersistenceUtils.saveObject((Serializable) original, file.toString(), tempDir.toString());
        @SuppressWarnings("unchecked")
        List<EncryptedPoint> loaded = PersistenceUtils.loadObject(file.toString(), tempDir.toString(), ArrayList.class);
        assertNotNull(loaded, "Loaded list should not be null");
        assertEquals(2, loaded.size(), "Expected 2 points in loaded list");
        assertEquals(point1.getId(), loaded.get(0).getId(), "First point ID mismatch");
        assertEquals(point2.getId(), loaded.get(1).getId(), "Second point ID mismatch");
        assertEquals(point1.getShardId(), loaded.get(0).getShardId(), "First point shard ID mismatch");
        assertEquals(point2.getShardId(), loaded.get(1).getShardId(), "Second point shard ID mismatch");
        assertArrayEquals(point1.getIv(), loaded.get(0).getIv(), "First point IV mismatch");
        assertArrayEquals(point2.getIv(), loaded.get(1).getIv(), "Second point IV mismatch");
        assertArrayEquals(point1.getCiphertext(), loaded.get(0).getCiphertext(), "First point ciphertext mismatch");
        assertArrayEquals(point2.getCiphertext(), loaded.get(1).getCiphertext(), "Second point ciphertext mismatch");
        assertEquals(point1.getVersion(), loaded.get(0).getVersion(), "First point version mismatch");
        assertEquals(point2.getVersion(), loaded.get(1).getVersion(), "Second point version mismatch");
        assertEquals(point1.getVectorLength(), loaded.get(0).getVectorLength(), "First point vector length mismatch");
        assertEquals(point2.getVectorLength(), loaded.get(1).getVectorLength(), "Second point vector length mismatch");
        assertEquals(point1.getBuckets(), loaded.get(0).getBuckets(), "First point buckets mismatch");
        assertEquals(point2.getBuckets(), loaded.get(1).getBuckets(), "Second point buckets mismatch");
    }

    @Test
    void loadMissingFileThrows() {
        Path file = tempDir.resolve("nofile.ser");
        assertThrows(IOException.class, () -> PersistenceUtils.loadObject(file.toString(), tempDir.toString(), EncryptedPoint.class),
                "Expected IOException for missing file");
    }
}