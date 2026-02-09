package com.fspann;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.RocksDBMetadataManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksDBException;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

@DisplayName("RocksDBMetadataManager Point Persistence Tests")
public class RocksDBMetadataManagerPointPersistenceTest {

    private RocksDBMetadataManager manager;

    @TempDir
    Path tempDir;

    @BeforeEach
    public void setUp() throws IOException, RocksDBException {
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
    @DisplayName("Test save and load encrypted point")
    public void testSaveAndLoadEncryptedPoint() throws IOException, ClassNotFoundException {
        String pointId = "v-1";  // Ensure ID is treated as a string
        EncryptedPoint point = new EncryptedPoint(
                pointId, 1, new byte[16], new byte[256],
                1, 128, 0, Arrays.asList(1, 2, 3), Collections.emptyList()
        );

        // Ensure the ID format is correct before saving
        assertTrue(pointId.matches("v-\\d+"), "Invalid point ID: " + pointId);

        manager.saveEncryptedPoint(point);
        EncryptedPoint loaded = manager.loadEncryptedPoint(pointId);

        assertNotNull(loaded);
        assertEquals(pointId, loaded.getId());
        assertEquals(128, loaded.getVectorLength());
    }

    @Test
    @DisplayName("Test batch save encrypted points")
    public void testBatchSavePoints() throws IOException, ClassNotFoundException {
        List<EncryptedPoint> points = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            EncryptedPoint point = new EncryptedPoint(
                    "v-" + i, 1, new byte[16], new byte[256],
                    1, 128, 0, Collections.emptyList(), Collections.emptyList()
            );
            points.add(point);
        }

        manager.saveEncryptedPointsBatch(points);

        for (int i = 0; i < 5; i++) {
            EncryptedPoint loaded = manager.loadEncryptedPoint("v-" + i);
            assertNotNull(loaded);
        }
    }

    @Test
    @DisplayName("Test get all encrypted points")
    public void testGetAllPoints() throws IOException {
        for (int i = 0; i < 3; i++) {
            EncryptedPoint point = new EncryptedPoint(
                    "v-" + i, 1, new byte[16], new byte[256],
                    1, 128, 0, Collections.emptyList(), Collections.emptyList()
            );
            manager.saveEncryptedPoint(point);
        }

        List<EncryptedPoint> all = manager.getAllEncryptedPoints();
        assertEquals(3, all.size());

        // Ensure that the ID is valid and in the expected format
        for (EncryptedPoint point : all) {
            assertTrue(point.getId().matches("v-\\d+"), "Invalid point ID: " + point.getId());
        }
    }


    @Test
    @DisplayName("Test cleanup stale metadata")
    public void testCleanupStale() throws IOException {
        for (int i = 0; i < 5; i++) {
            Map<String, String> meta = new HashMap<>();
            meta.put("id", "v-" + i);
            manager.updateVectorMetadata("v-" + i, meta);
        }

        Set<String> valid = new HashSet<>(Arrays.asList("v-0", "v-1", "v-2"));
        manager.cleanupStaleMetadata(valid);

        assertTrue(manager.getVectorMetadata("v-3").isEmpty());
    }
}