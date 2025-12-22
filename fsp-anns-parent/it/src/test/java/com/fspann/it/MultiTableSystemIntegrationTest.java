package com.fspann.it;

import com.fspann.api.ForwardSecureANNSystem;
import com.fspann.common.*;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.key.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Full System Integration Test with ForwardSecureANNSystem
 *
 * Tests complete end-to-end flow:
 * - File-based dataset loading
 * - Multi-table indexing
 * - Query execution
 * - Metrics collection
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MultiTableSystemIntegrationTest {

    private Path tempDir;
    private Path configPath;
    private Path dataPath;
    private Path metadataPath;
    private Path keysPath;
    private ForwardSecureANNSystem system;
    private RocksDBMetadataManager metadata;

    @BeforeAll
    void setUpAll() throws Exception {
        tempDir = Files.createTempDirectory("fspann-system-test");
        configPath = tempDir.resolve("config.json");
        dataPath = tempDir.resolve("data.fvecs");
        metadataPath = tempDir.resolve("metadata");
        keysPath = tempDir.resolve("keys.blob");

        Files.createDirectories(metadataPath);
        Files.createDirectories(metadataPath.resolve("metadata"));
        Files.createDirectories(metadataPath.resolve("points"));

        Files.writeString(configPath, """
        {
          "paper": {
            "enabled": true,
            "tables": 3,
            "divisions": 4,
            "m": 6,
            "lambda": 3,
            "seed": 12345,
            "probeLimit": 10
          },
          "runtime": {
            "maxCandidateFactor": 10,
            "maxRelaxationDepth": 2,
            "maxRefinementFactor": 3
          },
          "stabilization": {
            "enabled": true,
            "alpha": 0.8,
            "minCandidates": 50
          },
          "output": {
            "resultsDir": "results",
            "exportArtifacts": false
          },
          "eval": {
            "computePrecision": false,
            "writeGlobalPrecisionCsv": false
          },
          "partitionedIndexingEnabled": true,
          "opsThreshold": 1000000,
          "ageThresholdMs": 3600000
        }
        """);

        createSyntheticDataset(dataPath, 100, 6);
    }

    @BeforeEach
    void setUp() throws Exception {
        if (metadata != null) {
            metadata.close();
        }

        Path metaDB = metadataPath.resolve("metadata");
        Path pointsDB = metadataPath.resolve("points");

        deleteRecursively(metaDB);
        deleteRecursively(pointsDB);

        Files.createDirectories(metaDB);
        Files.createDirectories(pointsDB);

        metadata = RocksDBMetadataManager.create(
                metaDB.toString(),
                pointsDB.toString()
        );

        KeyManager km = new KeyManager(keysPath.toString());
        KeyRotationServiceImpl keyService = new KeyRotationServiceImpl(
                km,
                new KeyRotationPolicy(Integer.MAX_VALUE, Long.MAX_VALUE),
                metaDB.toString(),
                metadata,
                null
        );

        AesGcmCryptoService crypto = new AesGcmCryptoService(
                new SimpleMeterRegistry(),
                keyService,
                metadata
        );
        keyService.setCryptoService(crypto);

        system = new ForwardSecureANNSystem(
                configPath.toString(),
                dataPath.toString(),
                keysPath.toString(),
                List.of(6),
                metadataPath,
                false,
                metadata,
                crypto,
                50
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        if (system != null) {
            system.shutdown();
        }
        if (metadata != null) {
            metadata.close();
        }
    }

    @AfterAll
    void tearDownAll() throws Exception {
        deleteRecursively(tempDir);
    }

    @Test
    @DisplayName("Test 1: Multi-table token generation")
    void testTokenGeneration() {
        double[] query = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0};
        int topK = 10;
        int dim = 6;

        QueryToken token = system.createToken(query, topK, dim);

        assertNotNull(token);
        assertEquals(topK, token.getTopK());
        assertEquals(dim, token.getDimension());

        BitSet[][] codesByTable = token.getCodesByTable();
        assertNotNull(codesByTable);

        int L = 3;
        assertEquals(L, codesByTable.length);

        int divisions = 4;
        for (int t = 0; t < L; t++) {
            assertNotNull(codesByTable[t]);
            assertEquals(divisions, codesByTable[t].length);

            for (int d = 0; d < divisions; d++) {
                assertNotNull(codesByTable[t][d]);
            }
        }
    }

    @Test
    @DisplayName("Test 2: Table independence verification")
    void testTableIndependence() {
        double[] query = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0};

        QueryToken token = system.createToken(query, 10, 6);
        BitSet[][] codes = token.getCodesByTable();

        boolean foundDifference = false;
        for (int d = 0; d < Math.min(codes[0].length, codes[1].length); d++) {
            if (!codes[0][d].equals(codes[1][d])) {
                foundDifference = true;
                break;
            }
        }

        assertTrue(foundDifference);
    }

    @Test
    @DisplayName("Test 3: End-to-end indexing and querying")
    void testEndToEndFlow() throws Exception {
        system.indexStream(dataPath.toString(), 6);
        system.finalizeForSearch();

        double[] query = {5.0, 5.0, 5.0, 5.0, 5.0, 5.0};
        QueryToken token = system.createToken(query, 5, 6);

        List<QueryResult> results = system.getQueryServiceImpl().search(token);

        assertNotNull(results);
        assertTrue(results.size() > 0);
        assertTrue(results.size() <= 5);

        for (int i = 0; i < results.size() - 1; i++) {
            assertTrue(results.get(i).getDistance() <= results.get(i + 1).getDistance());
        }

        assertTrue(system.getQueryServiceImpl().getLastCandTotal() > 0);
    }

    @Test
    @DisplayName("Test 4: Multi-table candidate retrieval")
    void testCandidateRetrieval() throws Exception {
        system.indexStream(dataPath.toString(), 6);
        system.finalizeForSearch();

        double[] query = {3.0, 3.0, 3.0, 3.0, 3.0, 3.0};
        QueryToken token = system.createToken(query, 10, 6);

        List<QueryResult> results = system.getQueryServiceImpl().search(token);

        int candidatesTotal = system.getQueryServiceImpl().getLastCandTotal();
        int candidatesDecrypted = system.getQueryServiceImpl().getLastCandDecrypted();

        assertTrue(candidatesTotal > 0);
        assertTrue(candidatesDecrypted >= results.size());
    }

    @Test
    @DisplayName("Test 5: Invalid topK handling")
    void testInvalidTopK() throws Exception {
        system.indexStream(dataPath.toString(), 6);
        system.finalizeForSearch();

        double[] query = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0};

        assertThrows(IllegalArgumentException.class, () -> {
            system.createToken(query, 0, 6);
        });
    }

    @Test
    @DisplayName("Test 6: Query before finalization")
    void testQueryBeforeFinalization() throws Exception {
        system.indexStream(dataPath.toString(), 6);

        double[] query = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0};
        QueryToken token = system.createToken(query, 5, 6);

        assertThrows(IllegalStateException.class, () -> {
            system.getQueryServiceImpl().search(token);
        });
    }

    @Test
    @DisplayName("Test 7: Multiple queries")
    void testMultipleQueries() throws Exception {
        system.indexStream(dataPath.toString(), 6);
        system.finalizeForSearch();

        double[][] queries = {
                {1.0, 1.0, 1.0, 1.0, 1.0, 1.0},
                {5.0, 5.0, 5.0, 5.0, 5.0, 5.0},
                {10.0, 10.0, 10.0, 10.0, 10.0, 10.0}
        };

        for (int i = 0; i < queries.length; i++) {
            QueryToken token = system.createToken(queries[i], 5, 6);
            List<QueryResult> results = system.getQueryServiceImpl().search(token);

            assertNotNull(results);
            assertTrue(results.size() <= 5);

            for (int j = 0; j < results.size() - 1; j++) {
                assertTrue(results.get(j).getDistance() <= results.get(j + 1).getDistance());
            }
        }
    }

    @Test
    @DisplayName("Test 8: Indexing metrics")
    void testIndexingMetrics() throws Exception {
        int vectorCount = 100;
        system.indexStream(dataPath.toString(), 6);

        int indexed = system.getIndexedVectorCount();
        assertEquals(vectorCount, indexed);
    }

    @Test
    @DisplayName("Test 9: Dimension mismatch handling")
    void testDimensionMismatch() {
        double[] query = {1.0, 2.0, 3.0};  // Wrong dimension

        assertThrows(IllegalArgumentException.class, () -> {
            system.createToken(query, 10, 6);
        });
    }

    @Test
    @DisplayName("Test 10: Null query handling")
    void testNullQuery() {
        assertThrows(NullPointerException.class, () -> {
            system.createToken(null, 10, 6);
        });
    }

    private void createSyntheticDataset(Path path, int count, int dim) throws Exception {
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(count * (4 + dim * 4));
        buf.order(java.nio.ByteOrder.LITTLE_ENDIAN);

        Random rnd = new Random(42);
        for (int i = 0; i < count; i++) {
            buf.putInt(dim);
            for (int d = 0; d < dim; d++) {
                buf.putFloat((float) (i + d * 0.1 + rnd.nextGaussian() * 0.01));
            }
        }

        Files.write(path, buf.array());
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