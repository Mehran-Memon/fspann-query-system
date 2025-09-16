package com.fspann.common;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class RocksDBMetadataManagerTest {
    private static final Logger logger = LoggerFactory.getLogger(RocksDBMetadataManagerTest.class);

    private RocksDBMetadataManager metadataManager;
    private Path dbDir;
    private Path pointsDir;

    @BeforeEach
    void setUp(@TempDir Path tempDir) throws IOException {
        dbDir = tempDir.resolve("rocksdb");
        pointsDir = tempDir.resolve("points");
        Files.createDirectories(dbDir);
        Files.createDirectories(pointsDir);
        metadataManager = RocksDBMetadataManager.create(dbDir.toString(), pointsDir.toString());
        logger.info("Initialized manager at {} and {}", dbDir, pointsDir);
    }

    @AfterEach
    void tearDown() throws Exception {
        logger.info("Tearing down DB at {} and points at {}", dbDir, pointsDir);
        if (metadataManager != null) {
            metadataManager.close();
        }

        // destroy RocksDB
        try (Options opts = new Options().setCreateIfMissing(true)) {
            RocksDB.destroyDB(dbDir.toString(), opts);
            logger.debug("Destroyed RocksDB at {}", dbDir);
        }

        // remove directories
        Files.walk(dbDir.getParent())
                .sorted(Comparator.reverseOrder())
                .forEach(path -> {
                    try { Files.deleteIfExists(path); }
                    catch (IOException e) { /* ignore */ }
                });
    }

    @Test
    void testPutAndGetVectorMetadata() {
        String id = "vec123";
        Map<String, String> meta = Map.of("shardId", "1", "version", "v1");
        metadataManager.updateVectorMetadata(id, meta);

        Map<String, String> result = metadataManager.getVectorMetadata(id);
        assertEquals("1", result.get("shardId"));
        assertEquals("v1", result.get("version"));
    }

    @Test
    void testOverwriteVectorMetadata() {
        String id = "vec123";
        metadataManager.updateVectorMetadata(id, Map.of("shardId", "1", "version", "v1"));
        metadataManager.updateVectorMetadata(id, Map.of("version", "v2"));

        Map<String, String> result = metadataManager.getVectorMetadata(id);
        assertEquals("v2", result.get("version"));
        assertNull(result.get("shardId"));
    }

    @Test
    void testRemoveVectorMetadata() {
        String id = "vecToRemove";
        metadataManager.updateVectorMetadata(id, Map.of("shardId", "2", "version", "v1"));
        metadataManager.removeVectorMetadata(id);
        assertTrue(metadataManager.getVectorMetadata(id).isEmpty());
    }

    @Test
    void testMergeVectorMetadata() {
        String id = "vec999";
        metadataManager.updateVectorMetadata(id, Map.of("shardId", "2", "version", "v1"));
        metadataManager.mergeVectorMetadata(id, Map.of("version", "v2", "label", "secure"));

        Map<String, String> merged = metadataManager.getVectorMetadata(id);
        assertEquals("v1", merged.get("version"));
        assertEquals("2", merged.get("shardId"));
        assertEquals("secure", merged.get("label"));
    }

    @Test
    void testSaveAndLoadEncryptedPoint() throws Exception {
        String id = "vecPoint";
        List<Integer> buckets = Arrays.asList(1,2,3);
        EncryptedPoint pt = new EncryptedPoint(id, 1, new byte[]{0}, new byte[]{1}, 1, 128, buckets);
        metadataManager.updateVectorMetadata(id, Map.of("shardId","1","version","v1"));
        metadataManager.saveEncryptedPoint(pt);

        List<EncryptedPoint> pts = metadataManager.getAllEncryptedPoints();
        assertEquals(1, pts.size());
        EncryptedPoint loaded = pts.get(0);
        assertEquals(id, loaded.getId());
        assertEquals(1, loaded.getShardId());
    }

    @Test
    void testCloseIdempotent() throws IOException {
        metadataManager.close();
        assertDoesNotThrow(() -> metadataManager.close());
    }
}
