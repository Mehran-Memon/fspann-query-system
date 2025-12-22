package com.fspann.it;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.index.paper.PartitionedIndexService;
import com.fspann.key.*;
import com.fspann.query.core.QueryTokenFactory;
import com.fspann.query.service.QueryServiceImpl;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Edge Cases and Advanced Scenarios Test
 *
 * Tests:
 * - Boundary conditions
 * - Stress scenarios
 * - Parameter variations
 * - Error recovery
 */
class MultiTableEdgeCasesIT {

    @Test
    @DisplayName("Test 1: Very large topK (larger than dataset)")
    void testLargeTopK() throws Exception {
        Path tempDir = Files.createTempDirectory("large-topk-test");

        try {
            TestSystem sys = createSystem(tempDir, 3, 4, 6, 3, 12345L);

            for (int i = 0; i < 10; i++) {
                double[] vec = new double[6];
                Arrays.fill(vec, i);
                sys.index.insert("vec_" + i, vec);
            }
            sys.index.finalizeForSearch();

            double[] query = {5.0, 5.0, 5.0, 5.0, 5.0, 5.0};
            QueryToken token = sys.tokenFactory.create(query, 100);  // topK > dataset size

            List<QueryResult> results = sys.queryService.search(token);

            assertNotNull(results);
            assertTrue(results.size() <= 10);

            sys.cleanup();

        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    @DisplayName("Test 2: Minimum topK (K=1)")
    void testMinimumTopK() throws Exception {
        Path tempDir = Files.createTempDirectory("min-topk-test");

        try {
            TestSystem sys = createSystem(tempDir, 3, 4, 6, 3, 12345L);

            for (int i = 0; i < 20; i++) {
                double[] vec = new double[6];
                Arrays.fill(vec, i);
                sys.index.insert("vec_" + i, vec);
            }
            sys.index.finalizeForSearch();

            double[] query = {10.0, 10.0, 10.0, 10.0, 10.0, 10.0};
            QueryToken token = sys.tokenFactory.create(query, 1);

            List<QueryResult> results = sys.queryService.search(token);

            assertNotNull(results);
            assertTrue(results.size() <= 1);

            sys.cleanup();

        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    @DisplayName("Test 3: All identical vectors")
    void testIdenticalVectors() throws Exception {
        Path tempDir = Files.createTempDirectory("identical-test");

        try {
            TestSystem sys = createSystem(tempDir, 3, 4, 6, 3, 12345L);

            double[] identical = {5.0, 5.0, 5.0, 5.0, 5.0, 5.0};
            for (int i = 0; i < 15; i++) {
                sys.index.insert("same_" + i, identical.clone());
            }
            sys.index.finalizeForSearch();

            double[] query = {5.0, 5.0, 5.0, 5.0, 5.0, 5.0};
            QueryToken token = sys.tokenFactory.create(query, 10);

            List<QueryResult> results = sys.queryService.search(token);

            assertNotNull(results);
            assertTrue(results.size() > 0);

            for (QueryResult r : results) {
                assertTrue(r.getDistance() < 1e-6);
            }

            sys.cleanup();

        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    @DisplayName("Test 4: Very sparse vectors (mostly zeros)")
    void testSparseVectors() throws Exception {
        Path tempDir = Files.createTempDirectory("sparse-test");

        try {
            TestSystem sys = createSystem(tempDir, 3, 4, 6, 3, 12345L);

            for (int i = 0; i < 20; i++) {
                double[] vec = new double[6];
                vec[i % 6] = 1.0;  // Only one non-zero element
                sys.index.insert("sparse_" + i, vec);
            }
            sys.index.finalizeForSearch();

            double[] query = new double[6];
            query[2] = 1.0;
            QueryToken token = sys.tokenFactory.create(query, 5);

            List<QueryResult> results = sys.queryService.search(token);

            assertNotNull(results);
            assertTrue(results.size() > 0);

            sys.cleanup();

        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    @DisplayName("Test 5: Very dense vectors (all large values)")
    void testDenseVectors() throws Exception {
        Path tempDir = Files.createTempDirectory("dense-test");

        try {
            TestSystem sys = createSystem(tempDir, 3, 4, 6, 3, 12345L);

            for (int i = 0; i < 20; i++) {
                double[] vec = new double[6];
                Arrays.fill(vec, 1000.0 + i);
                sys.index.insert("dense_" + i, vec);
            }
            sys.index.finalizeForSearch();

            double[] query = new double[6];
            Arrays.fill(query, 1010.0);
            QueryToken token = sys.tokenFactory.create(query, 5);

            List<QueryResult> results = sys.queryService.search(token);

            assertNotNull(results);
            assertTrue(results.size() > 0);

            sys.cleanup();

        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    @DisplayName("Test 6: Negative vector values")
    void testNegativeValues() throws Exception {
        Path tempDir = Files.createTempDirectory("negative-test");

        try {
            TestSystem sys = createSystem(tempDir, 3, 4, 6, 3, 12345L);

            for (int i = 0; i < 20; i++) {
                double[] vec = new double[6];
                Arrays.fill(vec, -10.0 - i);
                sys.index.insert("neg_" + i, vec);
            }
            sys.index.finalizeForSearch();

            double[] query = new double[6];
            Arrays.fill(query, -15.0);
            QueryToken token = sys.tokenFactory.create(query, 5);

            List<QueryResult> results = sys.queryService.search(token);

            assertNotNull(results);
            assertTrue(results.size() > 0);

            sys.cleanup();

        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    @DisplayName("Test 7: Mixed positive and negative values")
    void testMixedValues() throws Exception {
        Path tempDir = Files.createTempDirectory("mixed-test");

        try {
            TestSystem sys = createSystem(tempDir, 3, 4, 6, 3, 12345L);

            Random rnd = new Random(42);
            for (int i = 0; i < 20; i++) {
                double[] vec = new double[6];
                for (int d = 0; d < 6; d++) {
                    vec[d] = rnd.nextDouble() * 20 - 10;  // Range: [-10, 10]
                }
                sys.index.insert("mixed_" + i, vec);
            }
            sys.index.finalizeForSearch();

            double[] query = new double[6];
            for (int d = 0; d < 6; d++) {
                query[d] = rnd.nextDouble() * 20 - 10;
            }
            QueryToken token = sys.tokenFactory.create(query, 5);

            List<QueryResult> results = sys.queryService.search(token);

            assertNotNull(results);

            sys.cleanup();

        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    @DisplayName("Test 8: Very small values (near zero)")
    void testVerySmallValues() throws Exception {
        Path tempDir = Files.createTempDirectory("small-test");

        try {
            TestSystem sys = createSystem(tempDir, 3, 4, 6, 3, 12345L);

            for (int i = 0; i < 20; i++) {
                double[] vec = new double[6];
                Arrays.fill(vec, 1e-6 * i);
                sys.index.insert("small_" + i, vec);
            }
            sys.index.finalizeForSearch();

            double[] query = new double[6];
            Arrays.fill(query, 1e-5);
            QueryToken token = sys.tokenFactory.create(query, 5);

            List<QueryResult> results = sys.queryService.search(token);

            assertNotNull(results);

            sys.cleanup();

        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    @DisplayName("Test 9: Large dataset stress test")
    void testLargeDataset() throws Exception {
        Path tempDir = Files.createTempDirectory("large-dataset-test");

        try {
            TestSystem sys = createSystem(tempDir, 3, 4, 6, 3, 12345L);

            Random rnd = new Random(42);
            int largeCount = 1000;

            for (int i = 0; i < largeCount; i++) {
                double[] vec = new double[6];
                for (int d = 0; d < 6; d++) {
                    vec[d] = rnd.nextGaussian();
                }
                sys.index.insert("large_" + i, vec);
            }
            sys.index.finalizeForSearch();

            double[] query = new double[6];
            for (int d = 0; d < 6; d++) {
                query[d] = rnd.nextGaussian();
            }
            QueryToken token = sys.tokenFactory.create(query, 10);

            long startNs = System.nanoTime();
            List<QueryResult> results = sys.queryService.search(token);
            long durationNs = System.nanoTime() - startNs;

            assertNotNull(results);
            assertTrue(results.size() > 0);
            assertTrue(durationNs < 5_000_000_000L);  // < 5 seconds

            sys.cleanup();

        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    @DisplayName("Test 10: Multiple concurrent finalizations (sequential)")
    void testSequentialFinalizations() throws Exception {
        Path tempDir = Files.createTempDirectory("multi-finalize-test");

        try {
            TestSystem sys = createSystem(tempDir, 3, 4, 6, 3, 12345L);

            for (int i = 0; i < 10; i++) {
                double[] vec = new double[6];
                Arrays.fill(vec, i);
                sys.index.insert("vec_" + i, vec);
            }

            sys.index.finalizeForSearch();
            sys.index.finalizeForSearch();  // Second finalization should be safe

            assertTrue(sys.index.isFrozen());

            sys.cleanup();

        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    @DisplayName("Test 11: Query with different table counts")
    void testVariableTableCounts() throws Exception {
        for (int L : new int[]{1, 2, 4, 6}) {
            Path tempDir = Files.createTempDirectory("var-tables-L" + L);

            try {
                TestSystem sys = createSystem(tempDir, L, 4, 6, 3, 12345L);

                for (int i = 0; i < 20; i++) {
                    double[] vec = new double[6];
                    Arrays.fill(vec, i);
                    sys.index.insert("vec_" + i, vec);
                }
                sys.index.finalizeForSearch();

                double[] query = {10.0, 10.0, 10.0, 10.0, 10.0, 10.0};
                QueryToken token = sys.tokenFactory.create(query, 5);

                List<QueryResult> results = sys.queryService.search(token);

                assertNotNull(results);
                assertEquals(L, token.getCodesByTable().length);

                sys.cleanup();

            } finally {
                deleteRecursively(tempDir);
            }
        }
    }

    @Test
    @DisplayName("Test 12: Query with different division counts")
    void testVariableDivisions() throws Exception {
        for (int j : new int[]{2, 4, 8}) {
            Path tempDir = Files.createTempDirectory("var-divs-j" + j);

            try {
                TestSystem sys = createSystem(tempDir, 3, j, 6, 3, 12345L);

                for (int i = 0; i < 20; i++) {
                    double[] vec = new double[6];
                    Arrays.fill(vec, i);
                    sys.index.insert("vec_" + i, vec);
                }
                sys.index.finalizeForSearch();

                double[] query = {10.0, 10.0, 10.0, 10.0, 10.0, 10.0};
                QueryToken token = sys.tokenFactory.create(query, 5);

                List<QueryResult> results = sys.queryService.search(token);

                assertNotNull(results);
                assertEquals(j, token.getCodesByTable()[0].length);

                sys.cleanup();

            } finally {
                deleteRecursively(tempDir);
            }
        }
    }

    @Test
    @DisplayName("Test 13: Zero vector query")
    void testZeroVectorQuery() throws Exception {
        Path tempDir = Files.createTempDirectory("zero-query-test");

        try {
            TestSystem sys = createSystem(tempDir, 3, 4, 6, 3, 12345L);

            for (int i = 0; i < 20; i++) {
                double[] vec = new double[6];
                Arrays.fill(vec, i);
                sys.index.insert("vec_" + i, vec);
            }
            sys.index.finalizeForSearch();

            double[] query = new double[6];  // All zeros
            QueryToken token = sys.tokenFactory.create(query, 5);

            List<QueryResult> results = sys.queryService.search(token);

            assertNotNull(results);

            sys.cleanup();

        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    @DisplayName("Test 14: Result distance ordering")
    void testResultDistanceOrdering() throws Exception {
        Path tempDir = Files.createTempDirectory("order-test");

        try {
            TestSystem sys = createSystem(tempDir, 3, 4, 6, 3, 12345L);

            for (int i = 0; i < 30; i++) {
                double[] vec = new double[6];
                Arrays.fill(vec, i * 2.0);
                sys.index.insert("vec_" + i, vec);
            }
            sys.index.finalizeForSearch();

            double[] query = {25.0, 25.0, 25.0, 25.0, 25.0, 25.0};
            QueryToken token = sys.tokenFactory.create(query, 10);

            List<QueryResult> results = sys.queryService.search(token);

            assertNotNull(results);
            assertTrue(results.size() > 1);

            // Verify strict ascending distance order
            for (int i = 0; i < results.size() - 1; i++) {
                assertTrue(results.get(i).getDistance() <= results.get(i + 1).getDistance(),
                        String.format("Distance[%d]=%.4f should be <= Distance[%d]=%.4f",
                                i, results.get(i).getDistance(),
                                i+1, results.get(i + 1).getDistance()));
            }

            sys.cleanup();

        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    @DisplayName("Test 15: Metadata consistency after queries")
    void testMetadataConsistency() throws Exception {
        Path tempDir = Files.createTempDirectory("consistency-test");

        try {
            TestSystem sys = createSystem(tempDir, 3, 4, 6, 3, 12345L);

            for (int i = 0; i < 10; i++) {
                double[] vec = new double[6];
                Arrays.fill(vec, i);
                sys.index.insert("vec_" + i, vec);
            }
            sys.index.finalizeForSearch();

            double[] query = {5.0, 5.0, 5.0, 5.0, 5.0, 5.0};
            QueryToken token = sys.tokenFactory.create(query, 5);

            sys.queryService.search(token);

            // Verify all points still exist in metadata
            for (int i = 0; i < 10; i++) {
                EncryptedPoint ep = sys.metadata.loadEncryptedPoint("vec_" + i);
                assertNotNull(ep);
                assertEquals("vec_" + i, ep.getId());
            }

            sys.cleanup();

        } finally {
            deleteRecursively(tempDir);
        }
    }

    private static class TestSystem {
        PartitionedIndexService index;
        QueryTokenFactory tokenFactory;
        QueryServiceImpl queryService;
        RocksDBMetadataManager metadata;

        void cleanup() throws Exception {
            if (metadata != null) {
                metadata.close();
            }
        }
    }

    private TestSystem createSystem(Path baseDir, int tables, int divisions,
                                    int m, int lambda, long seed) throws Exception {

        Path metaPath = baseDir.resolve("metadata");
        Path pointsPath = baseDir.resolve("points");

        Files.createDirectories(metaPath);
        Files.createDirectories(pointsPath);

        RocksDBMetadataManager metadata = RocksDBMetadataManager.create(
                metaPath.toString(),
                pointsPath.toString()
        );

        Path cfgFile = baseDir.resolve("config.json");
        Files.writeString(cfgFile, String.format("""
        {
          "paper": {
            "enabled": true,
            "tables": %d,
            "divisions": %d,
            "m": %d,
            "lambda": %d,
            "seed": %d
          },
          "runtime": {
            "maxCandidateFactor": 10,
            "maxRelaxationDepth": 2,
            "maxRefinementFactor": 3
          },
          "partitionedIndexingEnabled": true
        }
        """, tables, divisions, m, lambda, seed));

        SystemConfig cfg = SystemConfig.load(cfgFile.toString(), true);

        KeyManager km = new KeyManager(baseDir.resolve("keys.blob").toString());
        KeyRotationServiceImpl keyService = new KeyRotationServiceImpl(
                km,
                new KeyRotationPolicy(Integer.MAX_VALUE, Long.MAX_VALUE),
                metaPath.toString(),
                metadata,
                null
        );

        AesGcmCryptoService cryptoService = new AesGcmCryptoService(
                new SimpleMeterRegistry(),
                keyService,
                metadata
        );
        keyService.setCryptoService(cryptoService);

        PartitionedIndexService index = new PartitionedIndexService(
                metadata,
                cfg,
                keyService,
                cryptoService
        );

        QueryTokenFactory tokenFactory = new QueryTokenFactory(
                cryptoService,
                keyService,
                index,
                cfg,
                divisions
        );

        QueryServiceImpl queryService = new QueryServiceImpl(
                index,
                cryptoService,
                keyService,
                tokenFactory,
                cfg
        );

        TestSystem sys = new TestSystem();
        sys.metadata = metadata;
        sys.index = index;
        sys.tokenFactory = tokenFactory;
        sys.queryService = queryService;

        return sys;
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