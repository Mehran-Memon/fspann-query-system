package com.fspann;

import com.fspann.common.EncryptedPoint;
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

@DisplayName("RocksDBMetadataManager List and Query Tests")
public class RocksDBMetadataManagerListTest {

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
    @DisplayName("Test get all vector IDs")
    public void testGetAllVectorIds() {
        for (int i = 0; i < 5; i++) {
            Map<String, String> meta = new HashMap<>();
            meta.put("id", "v-" + i);
            manager.updateVectorMetadata("v-" + i, meta);
        }

        List<String> ids = manager.getAllVectorIds();
        assertEquals(5, ids.size());
    }

    @Test
    @DisplayName("Test size points directory")
    public void testSizePointsDir() throws IOException {
        Path pointsPath = tempDir.resolve("points");
        Files.createDirectories(pointsPath);
        Path testFile = pointsPath.resolve("test.point");
        Files.write(testFile, new byte[1024]);

        long size = manager.sizePointsDir();
        assertTrue(size > 0);
    }

    @Test
    @DisplayName("Test audit drift detects consistency")
    public void testAuditDrift() throws IOException {
        Map<String, String> meta = new HashMap<>();
        meta.put("id", "v-1");
        meta.put("version", "1");
        meta.put("shardId", "0");
        manager.updateVectorMetadata("v-1", meta);

        EncryptedPoint point = new EncryptedPoint(
                "v-1", 1, new byte[16], new byte[256],
                1, 128, 0, Collections.emptyList(), Collections.emptyList()
        );
        manager.saveEncryptedPoint(point);

        RocksDBMetadataManager.DriftReport report = manager.auditDrift();
        assertEquals(1, report.metaCount);
        assertEquals(1, report.diskCount);
        assertTrue(report.onlyMeta.isEmpty());
        assertTrue(report.onlyDisk.isEmpty());
    }

    @Test
    @DisplayName("Test quick summary line")
    public void testQuickSummary() {
        String summary = manager.quickSummaryLine();
        assertNotNull(summary);
        assertTrue(summary.contains("RocksDB"));
    }
}