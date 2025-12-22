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
 * Core Multi-Table mSANNP Integration Test
 *
 * Tests fundamental multi-table functionality:
 * - Token generation with L tables
 * - Code independence across tables
 * - Multi-table index storage
 * - Candidate union from multiple tables
 */
class MultiTableMSANNPIntegrationTest {

    private Path tempDir;
    private RocksDBMetadataManager metadata;
    private PartitionedIndexService indexService;
    private QueryTokenFactory tokenFactory;
    private QueryServiceImpl queryService;
    private KeyRotationServiceImpl keyService;
    private AesGcmCryptoService cryptoService;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("fspann-multitable-test");
        Path metaPath = tempDir.resolve("metadata");
        Path pointsPath = tempDir.resolve("points");

        Files.createDirectories(metaPath);
        Files.createDirectories(pointsPath);

        metadata = RocksDBMetadataManager.create(
                metaPath.toString(),
                pointsPath.toString()
        );

        // Multi-table config: L=3 tables
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
            "maxCandidateFactor": 5,
            "maxRelaxationDepth": 2,
            "maxRefinementFactor": 3
          },
          "stabilization": {
            "enabled": true,
            "alpha": 0.8,
            "minCandidates": 10
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

        cryptoService = new AesGcmCryptoService(
                new SimpleMeterRegistry(),
                keyService,
                metadata
        );
        keyService.setCryptoService(cryptoService);

        indexService = new PartitionedIndexService(
                metadata,
                cfg,
                keyService,
                cryptoService
        );

        tokenFactory = new QueryTokenFactory(
                cryptoService,
                keyService,
                indexService,
                cfg,
                cfg.getPaper().divisions
        );

        queryService = new QueryServiceImpl(
                indexService,
                cryptoService,
                keyService,
                tokenFactory,
                cfg
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
    @DisplayName("Test 1: QueryToken contains codes for L tables")
    void testQueryTokenMultiTableStructure() {
        double[] query = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0};
        int topK = 10;

        QueryToken token = tokenFactory.create(query, topK);

        assertNotNull(token, "Token should not be null");
        assertNotNull(token.getCodesByTable(), "Codes by table should not be null");

        int L = 3;
        assertEquals(L, token.getCodesByTable().length,
                "Token should have exactly L=" + L + " tables");

        int divisions = 4;
        for (int t = 0; t < L; t++) {
            BitSet[] tableCodes = token.getCodesByTable()[t];
            assertNotNull(tableCodes, "Table " + t + " codes should not be null");
            assertEquals(divisions, tableCodes.length,
                    "Table " + t + " should have " + divisions + " divisions");

            for (int d = 0; d < divisions; d++) {
                assertNotNull(tableCodes[d],
                        "Table " + t + " division " + d + " should not be null");
            }
        }
    }

    @Test
    @DisplayName("Test 2: Multi-table codes use independent seeds")
    void testTableSeedIndependence() {
        double[] query = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0};

        QueryToken token = tokenFactory.create(query, 10);
        BitSet[][] codes = token.getCodesByTable();

        assertTrue(codes.length >= 2, "Need at least 2 tables");

        boolean foundDifference = false;
        for (int d = 0; d < Math.min(codes[0].length, codes[1].length); d++) {
            if (!codes[0][d].equals(codes[1][d])) {
                foundDifference = true;
                break;
            }
        }

        assertTrue(foundDifference,
                "Tables should have different codes (independent seeds)");
    }

    @Test
    @DisplayName("Test 3: Index stores vectors across all tables")
    void testMultiTableIndexStorage() throws Exception {
        // Use more vectors in a cluster to ensure LSH finds candidates
        for (int i = 0; i < 20; i++) {
            double[] vec = new double[6];
            // Create a cluster around [5, 5, 5, 5, 5, 5]
            for (int d = 0; d < 6; d++) {
                vec[d] = 5.0 + (i * 0.1);
            }
            indexService.insert((String) "vec_" + i, vec);
        }

        indexService.finalizeForSearch();

        assertTrue(indexService.isFrozen(), "Index should be finalized");

        // Query in the middle of the cluster
        double[] query = {5.5, 5.5, 5.5, 5.5, 5.5, 5.5};
        QueryToken token = tokenFactory.create(query, 5);

        List<String> candidates = indexService.lookupCandidateIds(token);
        assertNotNull(candidates);
        assertTrue(candidates.size() > 0,
                "Multi-table lookup should find candidates");
    }

    @Test
    @DisplayName("Test 4: Multi-table union retrieves candidates from all tables")
    void testCandidateUnionBehavior() throws Exception {
        // Create larger clusters to ensure LSH finds candidates
        // Cluster 1 around [1, 1, 1, 1, 1, 1]
        for (int i = 0; i < 15; i++) {
            double[] vec = new double[6];
            for (int d = 0; d < 6; d++) {
                vec[d] = 1.0 + (i * 0.05);
            }
            indexService.insert((String) "c1_" + i, vec);
        }

        // Cluster 2 around [10, 10, 10, 10, 10, 10]
        for (int i = 0; i < 15; i++) {
            double[] vec = new double[6];
            for (int d = 0; d < 6; d++) {
                vec[d] = 10.0 + (i * 0.05);
            }
            indexService.insert((String) "c2_" + i, vec);
        }

        indexService.finalizeForSearch();

        // Query in cluster 1
        double[] query = {1.25, 1.25, 1.25, 1.25, 1.25, 1.25};
        QueryToken token = tokenFactory.create(query, 5);

        List<String> candidates = indexService.lookupCandidateIds(token);

        assertNotNull(candidates);
        assertTrue(candidates.size() > 0);

        long cluster1Count = candidates.stream()
                .filter(i -> i.startsWith("c1_"))
                .count();

        assertTrue(cluster1Count > 0,
                "Should find candidates from nearby cluster");
    }

    @Test
    @DisplayName("Test 5: End-to-end query with multi-table")
    void testEndToEndQuery() throws Exception {
        // Create more vectors in two clusters for robust LSH
        // Cluster 1: around [2, 3, 4, 5, 6, 7]
        for (int i = 0; i < 15; i++) {
            double[] vec = new double[6];
            for (int d = 0; d < 6; d++) {
                vec[d] = (d + 2) + (i * 0.1);
            }
            indexService.insert((String) "id_" + i, vec);
        }

        // Cluster 2: around [10, 11, 12, 13, 14, 15]
        for (int i = 15; i < 25; i++) {
            double[] vec = new double[6];
            for (int d = 0; d < 6; d++) {
                vec[d] = (d + 10) + ((i - 15) * 0.1);
            }
            indexService.insert((String) "id_" + i, vec);
        }

        indexService.finalizeForSearch();

        // Query in cluster 1
        double[] query = {2.5, 3.5, 4.5, 5.5, 6.5, 7.5};
        QueryToken token = tokenFactory.create(query, 5);

        List<QueryResult> results = queryService.search(token);

        assertNotNull(results);
        assertTrue(results.size() > 0);
        assertTrue(results.size() <= 5);

        for (int i = 0; i < results.size() - 1; i++) {
            assertTrue(results.get(i).getDistance() <= results.get(i + 1).getDistance(),
                    "Results should be sorted by distance");
        }

        assertTrue(queryService.getLastCandTotal() > 0);
    }

    @Test
    @DisplayName("Test 6: Multi-table handles empty index")
    void testEmptyIndexHandling() throws Exception {
        indexService.finalizeForSearch();

        double[] query = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0};
        QueryToken token = tokenFactory.create(query, 10);

        List<QueryResult> results = queryService.search(token);

        assertNotNull(results);
        assertTrue(results.isEmpty());
        assertEquals(0, queryService.getLastCandTotal());
    }

    @Test
    @DisplayName("Test 7: Multi-table handles single vector")
    void testSingleVectorQuery() throws Exception {
        double[] singleVec = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0};
        indexService.insert((String) "only_vec", singleVec);
        indexService.finalizeForSearch();

        double[] query = {1.1, 2.1, 3.1, 4.1, 5.1, 6.1};
        QueryToken token = tokenFactory.create(query, 5);

        List<QueryResult> results = queryService.search(token);

        assertNotNull(results);
        assertEquals(1, results.size());
        assertEquals("only_vec", results.get(0).getId());
    }

    @Test
    @DisplayName("Test 8: Query metrics are consistent")
    void testQueryMetricsConsistency() throws Exception {
        Random rnd = new Random(42);
        for (int i = 0; i < 50; i++) {
            double[] vec = new double[6];
            for (int d = 0; d < 6; d++) {
                vec[d] = rnd.nextGaussian();
            }
            indexService.insert((String) "vec_" + i, vec);
        }
        indexService.finalizeForSearch();

        double[] query = new double[6];
        for (int d = 0; d < 6; d++) {
            query[d] = rnd.nextGaussian();
        }

        QueryToken token = tokenFactory.create(query, 10);
        List<QueryResult> results = queryService.search(token);

        int candTotal = queryService.getLastCandTotal();
        int candKept = queryService.getLastCandKept();
        int candDecrypted = queryService.getLastCandDecrypted();
        int returned = queryService.getLastReturned();

        assertTrue(candTotal >= candKept);
        assertTrue(candKept >= candDecrypted);
        assertTrue(candDecrypted >= returned);
        assertTrue(returned <= 10);
        assertEquals(results.size(), returned);
    }

    @Test
    @DisplayName("Test 9: Touch tracking across tables")
    void testTouchTracking() throws Exception {
        for (int i = 0; i < 20; i++) {
            double[] vec = new double[6];
            Arrays.fill(vec, i);
            indexService.insert((String) "vec_" + i, vec);
        }
        indexService.finalizeForSearch();

        double[] query = {5.0, 5.0, 5.0, 5.0, 5.0, 5.0};
        QueryToken token = tokenFactory.create(query, 5);

        List<QueryResult> results = queryService.search(token);

        Set<String> touchedIds = indexService.getLastTouchedIds();

        assertNotNull(touchedIds);
        assertTrue(touchedIds.size() > 0);

        for (QueryResult r : results) {
            assertTrue(touchedIds.contains(r.getId()),
                    "Returned result should be in touched set");
        }
    }

    @Test
    @DisplayName("Test 10: Duplicate vectors handled correctly")
    void testDuplicateVectors() throws Exception {
        double[] vec1 = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0};
        double[] vec2 = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0};
        double[] vec3 = {10.0, 11.0, 12.0, 13.0, 14.0, 15.0};

        indexService.insert((String) "dup_1", vec1);
        indexService.insert((String) "dup_2", vec2);
        indexService.insert((String) "unique", vec3);

        indexService.finalizeForSearch();

        double[] query = {1.1, 2.1, 3.1, 4.1, 5.1, 6.1};
        QueryToken token = tokenFactory.create(query, 3);

        List<QueryResult> results = queryService.search(token);

        assertNotNull(results);
        assertTrue(results.size() >= 2);

        Set<String> resultIds = new HashSet<>();
        for (QueryResult r : results) {
            resultIds.add(r.getId());
        }

        assertTrue(resultIds.contains("dup_1") || resultIds.contains("dup_2"));
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