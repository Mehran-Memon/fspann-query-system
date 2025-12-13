package com.fspann;

import com.fspann.common.RocksDBMetadataManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

@DisplayName("RocksDBMetadataManager Metadata CRUD Tests")
public class RocksDBMetadataManagerCrudTest {

    private RocksDBMetadataManager manager;
    private Path metadataPath;
    private Path pointsPath;

    @TempDir
    Path tempDir;

    @BeforeEach
    public void setUp() throws IOException {
        metadataPath = tempDir.resolve("metadata");
        pointsPath = tempDir.resolve("points");
        Files.createDirectories(metadataPath);
        Files.createDirectories(pointsPath);

        manager = RocksDBMetadataManager.create(metadataPath.toString(), pointsPath.toString());
    }

    @Test
    @DisplayName("Test store and retrieve vector metadata")
    public void testStoreAndRetrieve() {
        Map<String, String> meta = new HashMap<>();
        meta.put("dimension", "128");
        meta.put("shard", "1");

        manager.updateVectorMetadata("v-1", meta);
        Map<String, String> retrieved = manager.getVectorMetadata("v-1");

        assertEquals("128", retrieved.get("dimension"));
        assertEquals("1", retrieved.get("shard"));
    }

    @Test
    @DisplayName("Test retrieve non-existent metadata returns empty")
    public void testRetrieveNonExistent() {
        Map<String, String> meta = manager.getVectorMetadata("nonexistent");
        assertTrue(meta.isEmpty());
    }

    @Test
    @DisplayName("Test update vector metadata overwrites")
    public void testUpdateOverwrites() {
        Map<String, String> meta1 = new HashMap<>();
        meta1.put("dim", "128");
        manager.updateVectorMetadata("v-1", meta1);

        Map<String, String> meta2 = new HashMap<>();
        meta2.put("dim", "256");
        manager.updateVectorMetadata("v-1", meta2);

        Map<String, String> retrieved = manager.getVectorMetadata("v-1");
        assertEquals("256", retrieved.get("dim"));
    }

    @Test
    @DisplayName("Test merge vector metadata preserves existing")
    public void testMergeMetadata() {
        Map<String, String> meta1 = new HashMap<>();
        meta1.put("dim", "128");
        meta1.put("shard", "1");
        manager.updateVectorMetadata("v-1", meta1);

        Map<String, String> meta2 = new HashMap<>();
        meta2.put("version", "2");
        manager.mergeVectorMetadata("v-1", meta2);

        Map<String, String> retrieved = manager.getVectorMetadata("v-1");
        assertEquals("128", retrieved.get("dim"));
        assertEquals("2", retrieved.get("version"));
    }

    @Test
    @DisplayName("Test remove vector metadata deletes entry")
    public void testRemoveMetadata() {
        Map<String, String> meta = new HashMap<>();
        meta.put("dim", "128");
        manager.updateVectorMetadata("v-1", meta);

        manager.removeVectorMetadata("v-1");

        Map<String, String> retrieved = manager.getVectorMetadata("v-1");
        assertTrue(retrieved.isEmpty());
    }

    @Test
    @DisplayName("Test batch update multiple metadata entries")
    public void testBatchUpdate() throws IOException {
        Map<String, Map<String, String>> batch = new HashMap<>();

        for (int i = 0; i < 10; i++) {
            Map<String, String> meta = new HashMap<>();
            meta.put("dim", "128");
            meta.put("id", "v-" + i);
            batch.put("v-" + i, meta);
        }

        manager.batchUpdateVectorMetadata(batch);

        for (int i = 0; i < 10; i++) {
            Map<String, String> retrieved = manager.getVectorMetadata("v-" + i);
            assertEquals("128", retrieved.get("dim"));
        }
    }

    @Test
    @DisplayName("Test multi-get vector metadata")
    public void testMultiGetMetadata() {
        for (int i = 0; i < 5; i++) {
            Map<String, String> meta = new HashMap<>();
            meta.put("id", "v-" + i);
            manager.updateVectorMetadata("v-" + i, meta);
        }

        Collection<String> ids = Arrays.asList("v-0", "v-1", "v-2");
        Map<String, Map<String, String>> results = manager.multiGetVectorMetadata(ids);

        assertEquals(3, results.size());
    }
}