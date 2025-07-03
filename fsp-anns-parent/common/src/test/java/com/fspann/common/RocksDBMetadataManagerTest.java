package com.fspann.common;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class RocksDBMetadataManagerTest {
    private static final Logger logger = LoggerFactory.getLogger(RocksDBMetadataManagerTest.class);
    private RocksDBMetadataManager metadataManager;
    private Path tempDbPath;
    private Path tempPointsDir;

    @BeforeEach
    public void setup() throws Exception {
        tempDbPath = Files.createTempDirectory("rocksdb_test_");
        tempPointsDir = Files.createTempDirectory("points_test_");
        metadataManager = new RocksDBMetadataManager(tempDbPath.toString()) {
            @Override
            public void saveEncryptedPoint(EncryptedPoint pt) throws IOException {
                String versionStr = getVectorMetadata(pt.getId()).getOrDefault("version", "v1");
                if (!versionStr.matches("[a-zA-Z0-9_]+")) {
                    throw new IllegalArgumentException("Invalid version format: " + versionStr);
                }
                Path versionDir = tempPointsDir.resolve("v" + versionStr);
                Files.createDirectories(versionDir);
                Path filePath = versionDir.resolve(pt.getId() + ".point");
                PersistenceUtils.saveObject(pt, filePath.toString());
                logger.info("Saved encrypted point: {} at {}", pt.getId(), filePath);
            }

            @Override
            public List<EncryptedPoint> getAllEncryptedPoints() {
                List<EncryptedPoint> points = new ArrayList<>();
                try (var paths = Files.walk(tempPointsDir)) {
                    paths.filter(Files::isRegularFile).forEach(path -> {
                        try {
                            EncryptedPoint point = PersistenceUtils.loadObject(path.toString());
                            if (point != null) points.add(point);
                        } catch (IOException | ClassNotFoundException e) {
                            logger.error("Failed to load encrypted point from file: {}", path, e);
                        }
                    });
                } catch (IOException e) {
                    logger.error("Failed to read encrypted points directory {}", tempPointsDir, e);
                }
                return points;
            }
        };
        logger.info("Initialized RocksDB at {} and points at {}", tempDbPath, tempPointsDir);
    }

    @AfterEach
    public void tearDown() {
        logger.info("Cleaning up RocksDB at {} and points at {}", tempDbPath, tempPointsDir);
        if (metadataManager != null) {
            try {
                metadataManager.close();
            } catch (Exception e) {
                logger.warn("Error during metadataManager.close()", e);
            } finally {
                metadataManager = null;
            }
        }
        try {
            Files.walk(tempDbPath)
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException e) {
                            logger.error("Failed to delete {}", p, e);
                        }
                    });
        } catch (IOException e) {
            logger.error("Failed to clean up directory {}", tempDbPath, e);
        }

        try {
            Files.walk(tempPointsDir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException e) {
                            logger.error("Failed to delete {}", p, e);
                        }
                    });
        } catch (IOException e) {
            logger.error("Failed to clean up directory {}", tempPointsDir, e);
        }
    }

    @Test
    public void testPutAndGetVectorMetadata() throws Exception {
        String vectorId = "vec123";
        Map<String, String> metadata = Map.of("shardId", "1", "version", "v1");
        metadataManager.putVectorMetadata(vectorId, metadata);
        Map<String, String> retrievedMetadata = metadataManager.getVectorMetadata(vectorId);
        assertEquals("1", retrievedMetadata.get("shardId"));
        assertEquals("v1", retrievedMetadata.get("version"));
    }

    @Test
    public void testUpdateVectorMetadata() throws Exception {
        String vectorId = "vec123";
        metadataManager.putVectorMetadata(vectorId, Map.of("shardId", "1", "version", "v1"));
        metadataManager.updateVectorMetadata(vectorId, Map.of("version", "v2"));
        Map<String, String> updatedMetadata = metadataManager.getVectorMetadata(vectorId);
        assertEquals("1", updatedMetadata.get("shardId"));
        assertEquals("v2", updatedMetadata.get("version"));
    }

    @Test
    public void testGetAllEncryptedPoints() throws Exception {
        String vectorId = "vec123";
        EncryptedPoint point = new EncryptedPoint(vectorId, 1, new byte[]{0, 1, 2}, new byte[]{3, 4, 5}, 1, 128);
        metadataManager.putVectorMetadata(vectorId, Map.of("version", "v1", "shardId", "1"));
        metadataManager.saveEncryptedPoint(point);

        List<EncryptedPoint> points = metadataManager.getAllEncryptedPoints();
        assertFalse(points.isEmpty());
        assertEquals(1, points.size());
        assertEquals(vectorId, points.get(0).getId());
        assertEquals(1, points.get(0).getShardId());
        assertArrayEquals(new byte[]{0, 1, 2}, points.get(0).getIv());
        assertArrayEquals(new byte[]{3, 4, 5}, points.get(0).getCiphertext());
        assertEquals(1, points.get(0).getVersion());
        assertEquals(128, points.get(0).getVectorLength());
    }

    @Test
    void testPutAndGetMetadata(@TempDir Path tempDir) throws Exception {
        Path dbPath = tempDir.resolve("rocksdb");
        Path pointsPath = tempDir.resolve("points");

        try (RocksDBMetadataManager manager = new RocksDBMetadataManager(dbPath.toString(), pointsPath.toString())) {
            manager.putVectorMetadata("vec1", Map.of("shardId", "1", "version", "v1"));
            Map<String, String> metadata = manager.getVectorMetadata("vec1");
            assertEquals("1", metadata.get("shardId"));
        }
    }

    @Test
    public void testShutdown() {
        String vectorId = "vec123";
        metadataManager.putVectorMetadata(vectorId, Map.of("shardId", "1", "version", "v1"));
        metadataManager.close();
        assertThrows(IllegalStateException.class, () -> metadataManager.getVectorMetadata(vectorId));
        // Removed: assertTrue(metadata.isEmpty()); because metadata is not accessible post-close
    }

    @Test
    public void testRemoveVectorMetadata() throws Exception {
        String vectorId = "vec123";
        metadataManager.putVectorMetadata(vectorId, Map.of("shardId", "1", "version", "v1"));
        metadataManager.removeVectorMetadata(vectorId);
        assertTrue(metadataManager.getVectorMetadata(vectorId).isEmpty());
    }
}
