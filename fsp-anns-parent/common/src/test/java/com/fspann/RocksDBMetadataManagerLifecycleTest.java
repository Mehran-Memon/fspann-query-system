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

@DisplayName("RocksDBMetadataManager Lifecycle Tests")
public class RocksDBMetadataManagerLifecycleTest {

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
    }

    @Test
    @DisplayName("Test manager closes cleanly")
    public void testCloseCleanly() throws IOException {
        RocksDBMetadataManager manager = RocksDBMetadataManager.create(
                metadataPath.toString(), pointsPath.toString());
        manager.close();
    }

    @Test
    @DisplayName("Test manager reopens existing database")
    public void testReopenDatabase() throws IOException {
        RocksDBMetadataManager m1 = RocksDBMetadataManager.create(
                metadataPath.toString(), pointsPath.toString());

        Map<String, String> meta = new HashMap<>();
        meta.put("test", "value");
        m1.updateVectorMetadata("v-1", meta);
        m1.close();

        RocksDBMetadataManager m2 = RocksDBMetadataManager.create(
                metadataPath.toString(), pointsPath.toString());

        Map<String, String> retrieved = m2.getVectorMetadata("v-1");
        assertEquals("value", retrieved.get("test"));

        m2.close();
    }

    @Test
    @DisplayName("Test log stats")
    public void testLogStats() throws IOException {
        RocksDBMetadataManager manager = RocksDBMetadataManager.create(
                metadataPath.toString(), pointsPath.toString());

        manager.logStats();

        manager.close();
    }

    @Test
    @DisplayName("Test print summary")
    public void testPrintSummary() throws IOException {
        RocksDBMetadataManager manager = RocksDBMetadataManager.create(
                metadataPath.toString(), pointsPath.toString());

        manager.printSummary();

        manager.close();
    }
}