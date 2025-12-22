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
 * Multi-Table Performance and Correctness Validation
 *
 * Tests:
 * - Candidate ratio scaling with L
 * - Query latency bounds
 * - Candidate uniqueness
 * - Touch tracking
 * - High-dimensional data
 * - Nearest neighbor correctness
 */
class MultiTablePerformanceValidationIT {

    @Test
    @DisplayName("Test 1: Candidate ratio with L=1 vs L=3 vs L=5")
    void testCandidateRatioScaling() throws Exception {
        int[] tableCounts = {1, 3, 5};

        for (int L : tableCounts) {
            Path tempDir = Files.createTempDirectory("ratio-test-L" + L);

            try {
                TestSystem sys = createSystem(tempDir, L, 4, 6, 3, 12345L);

                Random rnd = new Random(42);
                for (int i = 0; i < 50; i++) {
                    double[] vec = new double[6];
                    for (int d = 0; d < 6; d++) {
                        vec[d] = rnd.nextGaussian();
                    }
                    sys.index.insert("vec_" + i, vec);
                }
                sys.index.finalizeForSearch();

                double[] query = new double[6];
                for (int d = 0; d < 6; d++) {
                    query[d] = rnd.nextGaussian();
                }

                QueryToken token = sys.tokenFactory.create(query, 10);
                List<QueryResult> results = sys.queryService.search(token);

                int candidatesExamined = sys.queryService.getLastCandDecrypted();
                double ratio = candidatesExamined / 10.0;

                assertTrue(ratio >= 1.0);
                assertTrue(ratio <= 15.0);
                assertTrue(candidatesExamined >= Math.min(10, results.size()));

                sys.cleanup();

            } finally {
                deleteRecursively(tempDir);
            }
        }
    }

    @Test
    @DisplayName("Test 2: Query latency scaling")
    void testLatencyScaling() throws Exception {
        int[] tableCounts = {1, 3, 5};
        Map<Integer, Long> latencies = new HashMap<>();

        for (int L : tableCounts) {
            Path tempDir = Files.createTempDirectory("latency-test-L" + L);

            try {
                TestSystem sys = createSystem(tempDir, L, 4, 6, 3, 12345L);

                for (int i = 0; i < 100; i++) {
                    double[] vec = new double[6];
                    Arrays.fill(vec, i);
                    sys.index.insert("vec_" + i, vec);
                }
                sys.index.finalizeForSearch();

                double[] warmup = {5.0, 5.0, 5.0, 5.0, 5.0, 5.0};
                QueryToken warmupToken = sys.tokenFactory.create(warmup, 10);
                sys.queryService.search(warmupToken);

                double[] query = {50.0, 50.0, 50.0, 50.0, 50.0, 50.0};
                QueryToken token = sys.tokenFactory.create(query, 10);

                long startNs = System.nanoTime();
                sys.queryService.search(token);
                long durationNs = System.nanoTime() - startNs;

                latencies.put(L, durationNs / 1_000_000L);

                assertTrue(durationNs < 1_000_000_000L);

                sys.cleanup();

            } finally {
                deleteRecursively(tempDir);
            }
        }

        long lat1 = latencies.get(1);
        long lat5 = latencies.get(5);

        assertTrue(lat5 < lat1 * 10);
    }

    @Test
    @DisplayName("Test 3: Candidate uniqueness")
    void testCandidateUniqueness() throws Exception {
        Path tempDir = Files.createTempDirectory("uniqueness-test");

        try {
            TestSystem sys = createSystem(tempDir, 3, 4, 6, 3, 12345L);

            for (int i = 0; i < 20; i++) {
                double[] vec = new double[6];
                Arrays.fill(vec, i);
                sys.index.insert("vec_" + i, vec);
            }
            sys.index.finalizeForSearch();

            double[] query = {10.0, 10.0, 10.0, 10.0, 10.0, 10.0};
            QueryToken token = sys.tokenFactory.create(query, 5);

            List<QueryResult> results = sys.queryService.search(token);

            Set<String> uniqueIds = new HashSet<>();
            for (QueryResult r : results) {
                assertTrue(uniqueIds.add(r.getId()));
            }

            assertEquals(results.size(), uniqueIds.size());

            sys.cleanup();

        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    @DisplayName("Test 4: Touch tracking")
    void testTouchTracking() throws Exception {
        Path tempDir = Files.createTempDirectory("touch-test");

        try {
            TestSystem sys = createSystem(tempDir, 3, 4, 6, 3, 12345L);

            for (int i = 0; i < 30; i++) {
                double[] vec = new double[6];
                Arrays.fill(vec, i * 0.5);
                sys.index.insert("vec_" + i, vec);
            }
            sys.index.finalizeForSearch();

            double[] query = {5.0, 5.0, 5.0, 5.0, 5.0, 5.0};
            QueryToken token = sys.tokenFactory.create(query, 10);

            List<QueryResult> results = sys.queryService.search(token);

            Set<String> touched = sys.index.getLastTouchedIds();

            assertNotNull(touched);
            assertTrue(touched.size() > 0);

            for (QueryResult r : results) {
                assertTrue(touched.contains(r.getId()));
            }

            sys.cleanup();

        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    @DisplayName("Test 5: High-dimensional data")
    void testHighDimensional() throws Exception {
        Path tempDir = Files.createTempDirectory("highdim-test");

        try {
            int highDim = 128;
            TestSystem sys = createSystem(tempDir, 3, 8, 12, 5, 12345L);

            Random rnd = new Random(42);
            for (int i = 0; i < 50; i++) {
                double[] vec = new double[highDim];
                for (int d = 0; d < highDim; d++) {
                    vec[d] = rnd.nextGaussian();
                }
                sys.index.insert("high_" + i, vec);
            }
            sys.index.finalizeForSearch();

            double[] query = new double[highDim];
            for (int d = 0; d < highDim; d++) {
                query[d] = rnd.nextGaussian();
            }

            QueryToken token = sys.tokenFactory.create(query, 10);
            List<QueryResult> results = sys.queryService.search(token);

            assertNotNull(results);
            assertTrue(results.size() > 0);
            assertTrue(results.size() <= 10);

            sys.cleanup();

        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    @DisplayName("Test 6: Nearest neighbor correctness")
    void testNearestNeighborCorrectness() throws Exception {
        Path tempDir = Files.createTempDirectory("correctness-test");

        try {
            TestSystem sys = createSystem(tempDir, 3, 4, 6, 3, 12345L);

            double[] exact = {5.0, 5.0, 5.0, 5.0, 5.0, 5.0};
            sys.index.insert("exact_match", exact);

            Random rnd = new Random(42);
            for (int i = 0; i < 20; i++) {
                double[] vec = new double[6];
                for (int d = 0; d < 6; d++) {
                    vec[d] = 100 + rnd.nextGaussian() * 10;
                }
                sys.index.insert("noise_" + i, vec);
            }

            sys.index.finalizeForSearch();

            double[] query = {5.01, 5.01, 5.01, 5.01, 5.01, 5.01};
            QueryToken token = sys.tokenFactory.create(query, 5);

            List<QueryResult> results = sys.queryService.search(token);

            boolean foundExact = results.stream()
                    .anyMatch(r -> r.getId().equals("exact_match"));

            assertTrue(foundExact);

            QueryResult first = results.get(0);
            if (first.getId().equals("exact_match")) {
                assertTrue(first.getDistance() < 0.1);
            }

            sys.cleanup();

        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    @DisplayName("Test 7: Empty result handling")
    void testEmptyResults() throws Exception {
        Path tempDir = Files.createTempDirectory("empty-test");

        try {
            TestSystem sys = createSystem(tempDir, 3, 4, 6, 3, 12345L);

            sys.index.finalizeForSearch();

            double[] query = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0};
            QueryToken token = sys.tokenFactory.create(query, 10);

            List<QueryResult> results = sys.queryService.search(token);

            assertNotNull(results);
            assertTrue(results.isEmpty());
            assertEquals(0, sys.queryService.getLastCandTotal());

            sys.cleanup();

        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    @DisplayName("Test 8: Single vector handling")
    void testSingleVector() throws Exception {
        Path tempDir = Files.createTempDirectory("single-test");

        try {
            TestSystem sys = createSystem(tempDir, 3, 4, 6, 3, 12345L);

            double[] singleVec = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0};
            sys.index.insert("only_vec", singleVec);
            sys.index.finalizeForSearch();

            double[] query = {1.1, 2.1, 3.1, 4.1, 5.1, 6.1};
            QueryToken token = sys.tokenFactory.create(query, 5);

            List<QueryResult> results = sys.queryService.search(token);

            assertNotNull(results);
            assertEquals(1, results.size());
            assertEquals("only_vec", results.get(0).getId());

            sys.cleanup();

        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    @DisplayName("Test 9: Metrics consistency")
    void testMetricsConsistency() throws Exception {
        Path tempDir = Files.createTempDirectory("metrics-test");

        try {
            TestSystem sys = createSystem(tempDir, 3, 4, 6, 3, 12345L);

            Random rnd = new Random(42);
            for (int i = 0; i < 50; i++) {
                double[] vec = new double[6];
                for (int d = 0; d < 6; d++) {
                    vec[d] = rnd.nextGaussian();
                }
                sys.index.insert("vec_" + i, vec);
            }
            sys.index.finalizeForSearch();

            double[] query = new double[6];
            for (int d = 0; d < 6; d++) {
                query[d] = rnd.nextGaussian();
            }

            QueryToken token = sys.tokenFactory.create(query, 10);
            List<QueryResult> results = sys.queryService.search(token);

            int candTotal = sys.queryService.getLastCandTotal();
            int candKept = sys.queryService.getLastCandKept();
            int candDecrypted = sys.queryService.getLastCandDecrypted();
            int returned = sys.queryService.getLastReturned();

            assertTrue(candTotal >= candKept);
            assertTrue(candKept >= candDecrypted);
            assertTrue(candDecrypted >= returned);
            assertTrue(returned <= 10);
            assertEquals(results.size(), returned);

            sys.cleanup();

        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    @DisplayName("Test 10: Duplicate vector handling")
    void testDuplicateHandling() throws Exception {
        Path tempDir = Files.createTempDirectory("dup-test");

        try {
            TestSystem sys = createSystem(tempDir, 3, 4, 6, 3, 12345L);

            double[] vec1 = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0};
            double[] vec2 = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0};
            double[] vec3 = {10.0, 11.0, 12.0, 13.0, 14.0, 15.0};

            sys.index.insert("dup_1", vec1);
            sys.index.insert("dup_2", vec2);
            sys.index.insert("unique", vec3);

            sys.index.finalizeForSearch();

            double[] query = {1.1, 2.1, 3.1, 4.1, 5.1, 6.1};
            QueryToken token = sys.tokenFactory.create(query, 3);

            List<QueryResult> results = sys.queryService.search(token);

            assertNotNull(results);
            assertTrue(results.size() >= 2);

            Set<String> resultIds = new HashSet<>();
            for (QueryResult r : results) {
                resultIds.add(r.getId());
            }

            assertTrue(resultIds.contains("dup_1") || resultIds.contains("dup_2"));

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