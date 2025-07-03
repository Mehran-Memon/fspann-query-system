package com.fspann.common;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PersistenceUtilsTest {
    @TempDir
    Path tempDir;

    @Test
    void saveAndLoadEncryptedPoint() throws IOException, ClassNotFoundException {
        Path file = tempDir.resolve("point.ser");
        EncryptedPoint original = new EncryptedPoint(
                "vec123", 1, new byte[]{0, 1, 2}, new byte[]{3, 4, 5}, 1, 128
        );

        PersistenceUtils.saveObject(original, file.toString());
        EncryptedPoint loaded = PersistenceUtils.loadObject(file.toString());
        assertNotNull(loaded, "Loaded point should not be null");
        assertEquals(original.getId(), loaded.getId(), "Point ID mismatch");
        assertEquals(original.getShardId(), loaded.getShardId(), "Shard ID mismatch");
        assertArrayEquals(original.getIv(), loaded.getIv(), "IV mismatch");
        assertArrayEquals(original.getCiphertext(), loaded.getCiphertext(), "Ciphertext mismatch");
        assertEquals(original.getVersion(), loaded.getVersion(), "Version mismatch");
        assertEquals(original.getVectorLength(), loaded.getVectorLength(), "Vector length mismatch");
    }

    @Test
    void saveAndLoadEncryptedPointList() throws IOException, ClassNotFoundException {
        Path file = tempDir.resolve("points.ser");
        EncryptedPoint point1 = new EncryptedPoint(
                "vec1", 1, new byte[]{0, 1, 2}, new byte[]{3, 4, 5}, 1, 128
        );
        EncryptedPoint point2 = new EncryptedPoint(
                "vec2", 1, new byte[]{6, 7, 8}, new byte[]{9, 10, 11}, 1, 128
        );
        List<EncryptedPoint> original = Arrays.asList(point1, point2);

        // Workaround: Explicit cast to Serializable
        PersistenceUtils.saveObject((Serializable) original, file.toString());
        List<EncryptedPoint> loaded = PersistenceUtils.loadObject(file.toString());
        assertNotNull(loaded, "Loaded list should not be null");
        assertEquals(2, loaded.size(), "Expected 2 points in loaded list");
        assertEquals(point1.getId(), loaded.get(0).getId(), "First point ID mismatch");
        assertEquals(point2.getId(), loaded.get(1).getId(), "Second point ID mismatch");
        assertArrayEquals(point1.getIv(), loaded.get(0).getIv(), "First point IV mismatch");
        assertArrayEquals(point2.getIv(), loaded.get(1).getIv(), "Second point IV mismatch");
        assertArrayEquals(point1.getCiphertext(), loaded.get(0).getCiphertext(), "First point ciphertext mismatch");
        assertArrayEquals(point2.getCiphertext(), loaded.get(1).getCiphertext(), "Second point ciphertext mismatch");
        assertEquals(point1.getVersion(), loaded.get(0).getVersion(), "First point version mismatch");
        assertEquals(point2.getVersion(), loaded.get(1).getVersion(), "Second point version mismatch");
        assertEquals(point1.getVectorLength(), loaded.get(0).getVectorLength(), "First point vector length mismatch");
        assertEquals(point2.getVectorLength(), loaded.get(1).getVectorLength(), "Second point vector length mismatch");
    }

    @Test
    void loadMissingFileThrows() {
        Path file = tempDir.resolve("nofile.ser");
        assertThrows(IOException.class, () -> PersistenceUtils.loadObject(file.toString()), "Expected IOException for missing file");
    }
}