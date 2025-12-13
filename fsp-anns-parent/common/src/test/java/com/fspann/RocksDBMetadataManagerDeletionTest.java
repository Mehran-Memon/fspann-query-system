package com.fspann;

import com.fspann.common.RocksDBMetadataManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

@DisplayName("RocksDBMetadataManager Deletion Tests")
public class RocksDBMetadataManagerDeletionTest {

    private RocksDBMetadataManager manager;

    @TempDir
    Path tempDir;

    @BeforeEach
    public void setUp() throws IOException {
        Path metadataPath = tempDir.resolve("metadata");
        Path pointsPath = tempDir.resolve("points");
        Files.createDirectories(metadataPath);
        Files.createDirectories(pointsPath);

        manager = RocksDBMetadataManager.create(metadataPath.toString(), pointsPath.toString());
    }

    @AfterEach
    public void tearDown() {
        if (manager != null) {
            try {
                manager.close();
            } catch (Exception ignore) {}
        }
    }

    @Test
    @DisplayName("Test mark vector as deleted")
    public void testMarkDeleted() {
        Map<String, String> meta = new HashMap<>();
        meta.put("deleted", "true");
        meta.put("deleted_at", String.valueOf(System.currentTimeMillis()));
        manager.updateVectorMetadata("v-1", meta);

        assertTrue(manager.isDeleted("v-1"));
    }

    @Test
    @DisplayName("Test isDeleted returns false for non-deleted")
    public void testIsDeletedFalse() {
        Map<String, String> meta = new HashMap<>();
        meta.put("deleted", "false");
        manager.updateVectorMetadata("v-1", meta);

        assertFalse(manager.isDeleted("v-1"));
    }

    @Test
    @DisplayName("Test isDeleted handles missing vector")
    public void testIsDeletedMissing() {
        assertFalse(manager.isDeleted("nonexistent"));
    }

    @Test
    @DisplayName("Test get deletion timestamp")
    public void testGetDeletedTimestamp() {
        long timestamp = System.currentTimeMillis();
        Map<String, String> meta = new HashMap<>();
        meta.put("deleted", "true");
        meta.put("deleted_at", String.valueOf(timestamp));
        manager.updateVectorMetadata("v-1", meta);

        long retrieved = manager.getDeletedTimestamp("v-1");
        assertEquals(timestamp, retrieved);
    }

    @Test
    @DisplayName("Test hard delete removes from metadata")
    public void testHardDelete() {
        Map<String, String> meta = new HashMap<>();
        meta.put("deleted", "true");
        manager.updateVectorMetadata("v-1", meta);

        manager.hardDeleteVector("v-1");

        Map<String, String> retrieved = manager.getVectorMetadata("v-1");
        assertTrue(retrieved.isEmpty());
    }

    @Test
    @DisplayName("Test count deleted vectors")
    public void testCountDeleted() {
        for (int i = 0; i < 5; i++) {
            Map<String, String> meta = new HashMap<>();
            meta.put("deleted", i % 2 == 0 ? "true" : "false");
            manager.updateVectorMetadata("v-" + i, meta);
        }

        int deleted = manager.countDeletedVectors();
        assertEquals(3, deleted);
    }
}