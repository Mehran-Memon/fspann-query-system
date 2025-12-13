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

@DisplayName("RocksDBMetadataManager Index Version Tests")
public class RocksDBMetadataManagerVersionTest {

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

    @Test
    @DisplayName("Test save and load index version")
    public void testSaveLoadIndexVersion() {
        manager.saveIndexVersion(5);

        int version = manager.loadIndexVersion();
        assertEquals(5, version);
    }

    @Test
    @DisplayName("Test get version of vector")
    public void testGetVersionOfVector() {
        Map<String, String> meta = new HashMap<>();
        meta.put("version", "3");
        manager.updateVectorMetadata("v-1", meta);

        int version = manager.getVersionOfVector("v-1");
        assertEquals(3, version);
    }

    @Test
    @DisplayName("Test get version returns -1 for missing")
    public void testGetVersionMissing() {
        int version = manager.getVersionOfVector("nonexistent");
        assertEquals(-1, version);
    }
}