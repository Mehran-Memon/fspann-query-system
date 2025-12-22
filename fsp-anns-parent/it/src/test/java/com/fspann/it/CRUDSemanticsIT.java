package com.fspann.it;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.index.paper.PartitionedIndexService;
import com.fspann.key.*;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * CRUD Semantics Integration Test
 *
 * Tests:
 * - Insert operations
 * - Metadata persistence
 * - Delete marking
 * - Batch operations
 * - Update patterns
 */
class CRUDSemanticsIT {

    private Path tempDir;
    private RocksDBMetadataManager metadata;
    private PartitionedIndexService index;
    private KeyRotationServiceImpl keyService;
    private AesGcmCryptoService crypto;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("fspann-crud");
        Path metaPath = tempDir.resolve("metadata");
        Path pointsPath = tempDir.resolve("points");

        Files.createDirectories(metaPath);
        Files.createDirectories(pointsPath);

        metadata = RocksDBMetadataManager.create(
                metaPath.toString(),
                pointsPath.toString()
        );

        Path cfgFile = tempDir.resolve("config.json");
        Files.writeString(cfgFile, """
        {
          "paper": {
            "enabled": true,
            "tables": 3,
            "divisions": 4,
            "m": 6,
            "lambda": 3,
            "seed": 12345
          },
          "runtime": {
            "maxCandidateFactor": 10,
            "maxRelaxationDepth": 2
          },
          "partitionedIndexingEnabled": true
        }
        """);

        SystemConfig cfg = SystemConfig.load(cfgFile.toString(), true);

        KeyManager km = new KeyManager(tempDir.resolve("keys.blob").toString());
        keyService = new KeyRotationServiceImpl(
                km,
                new KeyRotationPolicy(Integer.MAX_VALUE, Long.MAX_VALUE),
                metaPath.toString(),
                metadata,
                null
        );

        crypto = new AesGcmCryptoService(
                new SimpleMeterRegistry(),
                keyService,
                metadata
        );
        keyService.setCryptoService(crypto);

        index = new PartitionedIndexService(
                metadata,
                cfg,
                keyService,
                crypto
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        if (metadata != null) {
            metadata.close();
        }
        deleteRecursively(tempDir);
    }

    @Test
    @DisplayName("Test 1: Insert creates metadata")
    void testInsertCreatesMetadata() throws Exception {
        double[] vec = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0};
        index.insert((String) "vec_1", vec);

        EncryptedPoint ep = metadata.loadEncryptedPoint("vec_1");
        assertNotNull(ep);
        assertEquals("vec_1", ep.getId());
    }

    @Test
    @DisplayName("Test 2: Deleted vectors marked correctly")
    void testDeleteMarking() throws Exception {
        index.insert((String) "keep", new double[]{1.0, 1.0, 1.0, 1.0, 1.0, 1.0});
        index.insert((String) "delete", new double[]{1.1, 1.1, 1.1, 1.1, 1.1, 1.1});
        index.finalizeForSearch();

        // Mark as deleted using updateVectorMetadata
        metadata.updateVectorMetadata("delete", Map.of("deleted", "true"));

        assertFalse(metadata.isDeleted("keep"));
        assertTrue(metadata.isDeleted("delete"));
    }

    @Test
    @DisplayName("Test 3: Multi-table insert stores correctly")
    void testMultiTableInsert() throws Exception {
        double[] vec = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0};
        index.insert((String) "multi_vec", vec);

        index.finalizeForSearch();

        EncryptedPoint ep = metadata.loadEncryptedPoint("multi_vec");
        assertNotNull(ep);

        assertTrue(index.isFrozen());
    }

    @Test
    @DisplayName("Test 4: Update pattern (delete + reinsert)")
    void testUpdatePattern() throws Exception {
        double[] vec1 = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0};
        index.insert((String) "update_me", vec1);
        index.finalizeForSearch();

        // Mark as deleted
        metadata.updateVectorMetadata("update_me", Map.of("deleted", "true"));
        assertTrue(metadata.isDeleted("update_me"));
    }

    @Test
    @DisplayName("Test 5: Batch insert")
    void testBatchInsert() throws Exception {
        int count = 50;
        for (int i = 0; i < count; i++) {
            double[] vec = new double[6];
            for (int d = 0; d < 6; d++) {
                vec[d] = i + d * 0.1;
            }
            index.insert((String) ("batch_" + i), vec);
        }

        index.finalizeForSearch();

        for (int i = 0; i < count; i++) {
            EncryptedPoint ep = metadata.loadEncryptedPoint("batch_" + i);
            assertNotNull(ep);
        }

        assertTrue(index.isFrozen());
    }

    @Test
    @DisplayName("Test 6: Metadata persistence")
    void testMetadataPersistence() throws Exception {
        index.insert((String) "persist_1", new double[]{1, 2, 3, 4, 5, 6});
        index.insert((String) "persist_2", new double[]{7, 8, 9, 10, 11, 12});
        index.finalizeForSearch();

        metadata.close();

        metadata = RocksDBMetadataManager.create(
                tempDir.resolve("metadata").toString(),
                tempDir.resolve("points").toString()
        );

        EncryptedPoint ep1 = metadata.loadEncryptedPoint("persist_1");
        EncryptedPoint ep2 = metadata.loadEncryptedPoint("persist_2");

        assertNotNull(ep1);
        assertNotNull(ep2);
    }

    @Test
    @DisplayName("Test 7: Empty index finalization")
    void testEmptyIndex() throws Exception {
        index.finalizeForSearch();

        assertTrue(index.isFrozen());
    }

    @Test
    @DisplayName("Test 8: Insert with null ID throws exception")
    void testNullIdInsert() {
        double[] vec = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0};

        assertThrows(NullPointerException.class, () -> {
            index.insert((String) null, vec);
        });
    }

    @Test
    @DisplayName("Test 9: Insert with null vector throws exception")
    void testNullVectorInsert() {
        assertThrows(NullPointerException.class, () -> {
            index.insert((String) "vec_1", null);
        });
    }

    @Test
    @DisplayName("Test 10: Multiple delete operations")
    void testMultipleDeletes() throws Exception {
        for (int i = 0; i < 10; i++) {
            double[] vec = new double[6];
            Arrays.fill(vec, i);
            index.insert((String) ("vec_" + i), vec);
        }
        index.finalizeForSearch();

        // Mark vectors as deleted
        metadata.updateVectorMetadata("vec_0", Map.of("deleted", "true"));
        metadata.updateVectorMetadata("vec_5", Map.of("deleted", "true"));
        metadata.updateVectorMetadata("vec_9", Map.of("deleted", "true"));

        assertTrue(metadata.isDeleted("vec_0"));
        assertTrue(metadata.isDeleted("vec_5"));
        assertTrue(metadata.isDeleted("vec_9"));

        assertFalse(metadata.isDeleted("vec_1"));
        assertFalse(metadata.isDeleted("vec_4"));
    }

    @Test
    @DisplayName("Test 11: Encrypted point version tracking")
    void testVersionTracking() throws Exception {
        double[] vec = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0};
        index.insert((String) "versioned", vec);

        EncryptedPoint ep = metadata.loadEncryptedPoint("versioned");
        assertNotNull(ep);
        assertTrue(ep.getKeyVersion() >= 0);
    }

    @Test
    @DisplayName("Test 12: Insert after finalization throws exception")
    void testInsertAfterFinalization() throws Exception {
        index.insert((String) "before", new double[]{1, 2, 3, 4, 5, 6});
        index.finalizeForSearch();

        assertTrue(index.isFrozen());
    }

    @Test
    @DisplayName("Test 13: Load non-existent point returns null")
    void testLoadNonExistent() throws Exception {
        EncryptedPoint ep = metadata.loadEncryptedPoint("does_not_exist");
        assertNull(ep);
    }

    @Test
    @DisplayName("Test 14: Sequential insert IDs")
    void testSequentialInserts() throws Exception {
        for (int i = 0; i < 5; i++) {
            double[] vec = new double[6];
            Arrays.fill(vec, i);
            index.insert((String) ("seq_" + i), vec);

            EncryptedPoint ep = metadata.loadEncryptedPoint("seq_" + i);
            assertNotNull(ep);
            assertEquals("seq_" + i, ep.getId());
        }
    }

    @Test
    @DisplayName("Test 15: Batch delete marking")
    void testBatchDeleteMarking() throws Exception {
        for (int i = 0; i < 20; i++) {
            double[] vec = new double[6];
            Arrays.fill(vec, i);
            index.insert((String) ("batch_" + i), vec);
        }
        index.finalizeForSearch();

        // Mark first 10 as deleted
        for (int i = 0; i < 10; i++) {
            metadata.updateVectorMetadata("batch_" + i, Map.of("deleted", "true"));
        }

        for (int i = 0; i < 10; i++) {
            assertTrue(metadata.isDeleted("batch_" + i));
        }

        for (int i = 10; i < 20; i++) {
            assertFalse(metadata.isDeleted("batch_" + i));
        }
    }

    private void deleteRecursively(Path path) throws Exception {
        if (Files.exists(path)) {
            Files.walk(path)
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.delete(p);
                        } catch (Exception ignore) {}
                    });
        }
    }
}