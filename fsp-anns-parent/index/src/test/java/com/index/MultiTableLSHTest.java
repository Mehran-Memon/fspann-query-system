package com.index;

import com.fspann.index.lsh.MultiTableLSH;
import com.fspann.index.lsh.RandomProjectionLSH;
import org.junit.jupiter.api.*;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MultiTableLSHTest – Comprehensive Test Suite (String IDs)
 *
 * Tests for:
 *  • Basic insert/query operations
 *  • Distance ordering correctness
 *  • Candidate ratio tracking
 *  • Query statistics
 *  • Concurrent insert operations
 *  • Adaptive query behavior
 *  • Ratio stability
 *
 * @author FSP-ANNS Project
 * @version 2.0 (String IDs)
 */
class MultiTableLSHTest {

    private MultiTableLSH lsh;
    private int numTables = 20;
    private int numFunctions = 8;
    private int numBuckets = 1000;
    private int dimension = 128;

    @BeforeEach
    void setup() {
        lsh = new MultiTableLSH(numTables, numFunctions, numBuckets);
    }

    @AfterEach
    void cleanup() {
        lsh.clear();
    }

    // ====================================================================
    // TEST 1-5: BASIC OPERATIONS
    // ====================================================================

    @Test
    @DisplayName("Insert and retrieve single vector")
    void testInsertRetrieveSingleVector() {
        String id = "vec1";
        double[] vector = new double[dimension];
        for (int i = 0; i < dimension; i++) {
            vector[i] = Math.random();
        }

        // Must call init first
        RandomProjectionLSH hashFamily = new RandomProjectionLSH();
        hashFamily.init(dimension, numTables, numFunctions, numBuckets);
        lsh.init(dimension, hashFamily);

        lsh.insert(id, vector);

        assertEquals(1, lsh.getTotalVectorsIndexed());
        assertNotNull(lsh.getVector(id));
    }

    @Test
    @DisplayName("Insert multiple vectors")
    void testInsertMultipleVectors() {
        RandomProjectionLSH hashFamily = new RandomProjectionLSH();
        hashFamily.init(dimension, numTables, numFunctions, numBuckets);
        lsh.init(dimension, hashFamily);

        int numVectors = 100;
        for (int i = 0; i < numVectors; i++) {
            double[] vec = new double[dimension];
            for (int d = 0; d < dimension; d++) {
                vec[d] = Math.random();
            }
            lsh.insert("vec" + i, vec);
        }

        assertEquals(numVectors, lsh.getTotalVectorsIndexed());
    }

    @Test
    @DisplayName("Query returns valid candidates (String IDs)")
    void testQueryReturnsValidCandidates() {
        RandomProjectionLSH hashFamily = new RandomProjectionLSH();
        hashFamily.init(dimension, numTables, numFunctions, numBuckets);
        lsh.init(dimension, hashFamily);

        // Insert 50 vectors
        double[][] vectors = new double[50][dimension];
        for (int i = 0; i < 50; i++) {
            vectors[i] = new double[dimension];
            for (int d = 0; d < dimension; d++) {
                vectors[i][d] = Math.random();
            }
            lsh.insert("vec_" + i, vectors[i]);
        }

        // Query with first vector
        List<Map.Entry<String, Double>> results = lsh.query(vectors[0], 10);

        // Verify results
        assertNotNull(results);
        assertTrue(results.size() > 0);

        // All results must have String IDs
        for (Map.Entry<String, Double> entry : results) {
            String id = entry.getKey();
            Double dist = entry.getValue();

            // Verify String ID
            assertNotNull(id);
            assertTrue(id.startsWith("vec_"), "ID should be String starting with 'vec_'");

            // Verify distance
            assertNotNull(dist);
            assertTrue(dist >= 0, "Distance should be non-negative");
        }
    }

    @Test
    @DisplayName("Query with dimension mismatch throws error")
    void testQueryDimensionMismatch() {
        RandomProjectionLSH hashFamily = new RandomProjectionLSH();
        hashFamily.init(dimension, numTables, numFunctions, numBuckets);
        lsh.init(dimension, hashFamily);

        double[] query = new double[dimension + 1];  // Wrong dimension
        assertThrows(IllegalArgumentException.class, () -> lsh.query(query, 10));
    }

    @Test
    @DisplayName("Query with invalid topK throws error")
    void testQueryInvalidTopK() {
        RandomProjectionLSH hashFamily = new RandomProjectionLSH();
        hashFamily.init(dimension, numTables, numFunctions, numBuckets);
        lsh.init(dimension, hashFamily);

        double[] query = new double[dimension];
        assertThrows(IllegalArgumentException.class, () -> lsh.query(query, 0));
        assertThrows(IllegalArgumentException.class, () -> lsh.query(query, -1));
    }

    // ====================================================================
    // TEST 6-10: DISTANCE ORDERING
    // ====================================================================

    @Test
    @DisplayName("Query results ordered by distance (ascending)")
    void testQueryDistanceOrdering() {
        RandomProjectionLSH hashFamily = new RandomProjectionLSH();
        hashFamily.init(dimension, numTables, numFunctions, numBuckets);
        lsh.init(dimension, hashFamily);

        // Insert vectors
        double[][] vectors = new double[30][dimension];
        for (int i = 0; i < 30; i++) {
            vectors[i] = new double[dimension];
            for (int d = 0; d < dimension; d++) {
                vectors[i][d] = Math.random();
            }
            lsh.insert("vec_" + i, vectors[i]);
        }

        // Query and check ordering
        List<Map.Entry<String, Double>> results = lsh.query(vectors[0], 15);

        // Verify ordering
        for (int i = 0; i < results.size() - 1; i++) {
            double dist1 = results.get(i).getValue();
            double dist2 = results.get(i + 1).getValue();
            assertTrue(dist1 <= dist2 + 0.0001, // Small tolerance for floating point
                    "Results should be ordered by distance: " + dist1 + " should be <= " + dist2);
        }
    }

    @Test
    @DisplayName("Nearest neighbor has lowest distance")
    void testNearestNeighbor() {
        RandomProjectionLSH hashFamily = new RandomProjectionLSH();
        hashFamily.init(dimension, numTables, numFunctions, numBuckets);
        lsh.init(dimension, hashFamily);

        double[] query = new double[dimension];
        double[] nearestVec = new double[dimension];

        // Create query and nearest vector (very close)
        for (int d = 0; d < dimension; d++) {
            query[d] = 0.5;
            nearestVec[d] = 0.5 + 0.001 * Math.random();
        }

        lsh.insert("nearest", nearestVec);

        // Add some random vectors
        for (int i = 0; i < 20; i++) {
            double[] vec = new double[dimension];
            for (int d = 0; d < dimension; d++) {
                vec[d] = Math.random();
            }
            lsh.insert("vec_" + i, vec);
        }

        List<Map.Entry<String, Double>> results = lsh.query(query, 10);

        // Nearest should have lowest distance
        assertEquals("nearest", results.get(0).getKey(),
                "Nearest neighbor should be first in results");
    }

    // ====================================================================
    // TEST 11-15: CANDIDATE RATIO TRACKING
    // ====================================================================

    @Test
    @DisplayName("Candidate ratio computed correctly")
    void testCandidateRatioTracking() {
        RandomProjectionLSH hashFamily = new RandomProjectionLSH();
        hashFamily.init(dimension, numTables, numFunctions, numBuckets);
        lsh.init(dimension, hashFamily);

        // Insert vectors
        for (int i = 0; i < 100; i++) {
            double[] vec = new double[dimension];
            for (int d = 0; d < dimension; d++) {
                vec[d] = Math.random();
            }
            lsh.insert("vec_" + i, vec);
        }

        // Query
        double[] query = new double[dimension];
        for (int d = 0; d < dimension; d++) {
            query[d] = Math.random();
        }

        List<Map.Entry<String, Double>> results = lsh.query(query, 10);

        // Ratio = candidates / topK
        int candidates = results.size();
        double ratio = (double) candidates / 10;

        assertTrue(ratio >= 1.0, "Ratio should be >= 1.0");
        assertTrue(ratio <= 10.0, "Ratio should be <= 10.0 for reasonable LSH");
    }

    @Test
    @DisplayName("Query statistics available after query")
    void testStatisticsTracking() {
        RandomProjectionLSH hashFamily = new RandomProjectionLSH();
        hashFamily.init(dimension, numTables, numFunctions, numBuckets);
        lsh.init(dimension, hashFamily);

        // Insert and query
        for (int i = 0; i < 50; i++) {
            double[] vec = new double[dimension];
            for (int d = 0; d < dimension; d++) {
                vec[d] = Math.random();
            }
            lsh.insert("vec_" + i, vec);
        }

        double[] query = new double[dimension];
        for (int d = 0; d < dimension; d++) {
            query[d] = Math.random();
        }
        lsh.query(query, 10);

        // Get statistics
        Map<String, Double> stats = lsh.getQueryStatistics(10);

        assertNotNull(stats);
        assertTrue(stats.containsKey("avg_ratio"));
        assertTrue(stats.containsKey("count"));
        assertTrue(stats.get("avg_ratio") > 0);
    }

    // ====================================================================
    // TEST 16-18: CONCURRENT OPERATIONS
    // ====================================================================

    @Test
    @DisplayName("Concurrent inserts are thread-safe")
    void testConcurrentInsert() throws InterruptedException {
        RandomProjectionLSH hashFamily = new RandomProjectionLSH();
        hashFamily.init(dimension, numTables, numFunctions, numBuckets);
        lsh.init(dimension, hashFamily);

        int numThreads = 4;
        int vectorsPerThread = 25;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                for (int i = 0; i < vectorsPerThread; i++) {
                    double[] vec = new double[dimension];
                    for (int d = 0; d < dimension; d++) {
                        vec[d] = Math.random();
                    }
                    lsh.insert("vec_" + threadId + "_" + i, vec);
                }
            });
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));

        assertEquals(numThreads * vectorsPerThread, lsh.getTotalVectorsIndexed());
    }

    @Test
    @DisplayName("Concurrent queries are safe")
    void testConcurrentQuery() throws InterruptedException {
        RandomProjectionLSH hashFamily = new RandomProjectionLSH();
        hashFamily.init(dimension, numTables, numFunctions, numBuckets);
        lsh.init(dimension, hashFamily);

        // Insert vectors
        for (int i = 0; i < 50; i++) {
            double[] vec = new double[dimension];
            for (int d = 0; d < dimension; d++) {
                vec[d] = Math.random();
            }
            lsh.insert("vec_" + i, vec);
        }

        // Concurrent queries
        int numThreads = 4;
        int queriesPerThread = 10;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads * queriesPerThread);

        for (int t = 0; t < numThreads; t++) {
            executor.submit(() -> {
                for (int q = 0; q < queriesPerThread; q++) {
                    double[] query = new double[dimension];
                    for (int d = 0; d < dimension; d++) {
                        query[d] = Math.random();
                    }
                    List<Map.Entry<String, Double>> results = lsh.query(query, 10);
                    assertNotNull(results);
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        executor.shutdown();
    }

    // ====================================================================
    // TEST 19-20: ADAPTIVE QUERY
    // ====================================================================

    @Test
    @DisplayName("Adaptive query returns results (String IDs)")
    void testAdaptiveQuery() {
        RandomProjectionLSH hashFamily = new RandomProjectionLSH();
        hashFamily.init(dimension, numTables, numFunctions, numBuckets);
        lsh.init(dimension, hashFamily);

        // Insert vectors
        for (int i = 0; i < 50; i++) {
            double[] vec = new double[dimension];
            for (int d = 0; d < dimension; d++) {
                vec[d] = Math.random();
            }
            lsh.insert("vec_" + i, vec);
        }

        // Adaptive query
        double[] query = new double[dimension];
        for (int d = 0; d < dimension; d++) {
            query[d] = Math.random();
        }

        List<Map.Entry<String, Double>> results = lsh.adaptiveQuery(query, 10);

        // Verify String IDs in results
        assertNotNull(results);
        for (Map.Entry<String, Double> entry : results) {
            String id = entry.getKey();
            assertNotNull(id);
            assertTrue(id.startsWith("vec_"), "Should have String ID");
        }
    }

    @Test
    @DisplayName("Ratio stability across multiple queries")
    void testRatioStability() {
        RandomProjectionLSH hashFamily = new RandomProjectionLSH();
        hashFamily.init(dimension, numTables, numFunctions, numBuckets);
        lsh.init(dimension, hashFamily);

        // Insert vectors
        for (int i = 0; i < 100; i++) {
            double[] vec = new double[dimension];
            for (int d = 0; d < dimension; d++) {
                vec[d] = Math.random();
            }
            lsh.insert("vec_" + i, vec);
        }

        // Multiple queries
        List<Double> ratios = new ArrayList<>();
        for (int q = 0; q < 20; q++) {
            double[] query = new double[dimension];
            for (int d = 0; d < dimension; d++) {
                query[d] = Math.random();
            }

            List<Map.Entry<String, Double>> results = lsh.query(query, 10);
            double ratio = (double) results.size() / 10;
            ratios.add(ratio);
        }

        // Check stability (std dev should be small)
        double mean = ratios.stream().mapToDouble(Double::doubleValue).average().orElse(0);
        double variance = ratios.stream()
                .mapToDouble(r -> (r - mean) * (r - mean))
                .average()
                .orElse(0);
        double stdDev = Math.sqrt(variance);

        assertTrue(stdDev < mean, "Ratio should be relatively stable");
    }

    // ====================================================================
    // TEST 21-22: UTILITY METHODS
    // ====================================================================

    @Test
    @DisplayName("Clear removes all vectors")
    void testClear() {
        RandomProjectionLSH hashFamily = new RandomProjectionLSH();
        hashFamily.init(dimension, numTables, numFunctions, numBuckets);
        lsh.init(dimension, hashFamily);

        // Insert vectors
        for (int i = 0; i < 20; i++) {
            double[] vec = new double[dimension];
            for (int d = 0; d < dimension; d++) {
                vec[d] = Math.random();
            }
            lsh.insert("vec_" + i, vec);
        }

        assertEquals(20, lsh.getTotalVectorsIndexed());

        lsh.clear();

        assertEquals(0, lsh.getTotalVectorsIndexed());
    }

    @Test
    @DisplayName("toString provides diagnostic info")
    void testToString() {
        RandomProjectionLSH hashFamily = new RandomProjectionLSH();
        hashFamily.init(dimension, numTables, numFunctions, numBuckets);
        lsh.init(dimension, hashFamily);

        String info = lsh.toString();

        assertNotNull(info);
        assertTrue(info.contains("MultiTableLSH"));
        assertTrue(info.contains(String.valueOf(dimension)));
    }
}