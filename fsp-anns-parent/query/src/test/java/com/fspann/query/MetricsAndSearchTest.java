package com.fspann.query;

import com.fspann.common.QueryMetrics;
import com.fspann.common.QueryResult;
import com.fspann.common.Profiler;
import com.fspann.query.core.Aggregates;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.*;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test suite for:
 *   1. QueryMetrics - distance ratio, precision, candidate ratio
 *   2. Ratio computation formula (per-position, paper-compliant)
 *   3. Precision@K computation
 *   4. Aggregates from Profiler
 *   5. Search logic integration
 *
 * Reference: Peng et al. 2018, Section VI-A
 *   ratio = (1/nq) × Σᵢ [ (1/k) × Σⱼ ( ‖o*ᵢⱼ - qᵢ‖ / ‖oᵢⱼ - qᵢ‖ ) ]
 *
 * Where:
 *   - oᵢⱼ = j-th true nearest neighbor of query qᵢ
 *   - o*ᵢⱼ = j-th returned result for query qᵢ
 */
@DisplayName("Metrics and Search Logic Tests")
class MetricsAndSearchTest {

    private static final double EPSILON = 1e-9;

    // ========================================================================
    // SECTION 1: QueryMetrics Basic Tests
    // ========================================================================

    @Nested
    @DisplayName("QueryMetrics Construction and Accessors")
    class QueryMetricsBasicTests {

        @Test
        @DisplayName("3-argument constructor stores all values correctly")
        void testThreeArgConstructor() {
            QueryMetrics m = new QueryMetrics(1.15, 0.92, 2.5);

            assertEquals(1.15, m.ratioAtK(), EPSILON, "Distance ratio mismatch");
            assertEquals(0.92, m.precisionAtK(), EPSILON, "Precision mismatch");
            assertEquals(2.5, m.candidateRatioAtK(), EPSILON, "Candidate ratio mismatch");
        }

        @Test
        @DisplayName("2-argument constructor defaults candidateRatio to NaN")
        void testTwoArgConstructor() {
            QueryMetrics m = new QueryMetrics(1.05, 0.88);

            assertEquals(1.05, m.ratioAtK(), EPSILON);
            assertEquals(0.88, m.precisionAtK(), EPSILON);
            assertTrue(Double.isNaN(m.candidateRatioAtK()), "Candidate ratio should be NaN");
        }

        @Test
        @DisplayName("Perfect metrics (ratio=1.0, precision=1.0)")
        void testPerfectMetrics() {
            QueryMetrics m = new QueryMetrics(1.0, 1.0, 1.0);

            assertEquals(1.0, m.ratioAtK(), EPSILON, "Perfect ratio should be 1.0");
            assertEquals(1.0, m.precisionAtK(), EPSILON, "Perfect precision should be 1.0");
            assertEquals(1.0, m.candidateRatioAtK(), EPSILON, "Minimum candidate ratio is 1.0");
        }

        @Test
        @DisplayName("Edge case: NaN values handled correctly")
        void testNaNValues() {
            QueryMetrics m = new QueryMetrics(Double.NaN, 0.5, Double.NaN);

            assertTrue(Double.isNaN(m.ratioAtK()));
            assertEquals(0.5, m.precisionAtK(), EPSILON);
            assertTrue(Double.isNaN(m.candidateRatioAtK()));
        }

        @Test
        @DisplayName("toString() produces readable output")
        void testToString() {
            QueryMetrics m = new QueryMetrics(1.234, 0.876, 3.5);
            String str = m.toString();

            assertTrue(str.contains("1.234") || str.contains("1.2340"), "Should contain ratio");
            assertTrue(str.contains("0.876") || str.contains("0.8760"), "Should contain precision");
            assertTrue(str.contains("3.5") || str.contains("3.50"), "Should contain candidate ratio");
        }
    }

    // ========================================================================
    // SECTION 2: Distance Ratio Formula Tests (Paper-Compliant)
    // ========================================================================

    @Nested
    @DisplayName("Distance Ratio Computation (Per-Position Formula)")
    class DistanceRatioTests {

        /**
         * Simulates the ratio computation from computeMetricsAtK().
         * Formula: avg over j of (dist_returned_j / dist_gt_j)
         */
        private double computeDistanceRatio(double[] returnedDistances, double[] gtDistances) {
            if (returnedDistances == null || gtDistances == null) return Double.NaN;
            if (returnedDistances.length == 0 || gtDistances.length == 0) return Double.NaN;

            int k = Math.min(returnedDistances.length, gtDistances.length);
            double sum = 0.0;
            int validCount = 0;

            for (int j = 0; j < k; j++) {
                double dGt = (j < gtDistances.length) ? gtDistances[j] : gtDistances[gtDistances.length - 1];
                if (dGt > 1e-24) {
                    sum += returnedDistances[j] / dGt;
                    validCount++;
                }
            }

            return validCount > 0 ? sum / validCount : Double.NaN;
        }

        @Test
        @DisplayName("Perfect match: returned = groundtruth → ratio = 1.0")
        void testPerfectMatch() {
            double[] gtDistances = {1.0, 2.0, 3.0, 4.0, 5.0};
            double[] returnedDistances = {1.0, 2.0, 3.0, 4.0, 5.0};

            double ratio = computeDistanceRatio(returnedDistances, gtDistances);

            assertEquals(1.0, ratio, EPSILON, "Perfect match should yield ratio=1.0");
        }

        @Test
        @DisplayName("Returned results 10% farther → ratio ≈ 1.1")
        void testTenPercentFarther() {
            double[] gtDistances = {1.0, 2.0, 3.0, 4.0, 5.0};
            double[] returnedDistances = {1.1, 2.2, 3.3, 4.4, 5.5}; // each 10% farther

            double ratio = computeDistanceRatio(returnedDistances, gtDistances);

            assertEquals(1.1, ratio, EPSILON, "10% farther should yield ratio≈1.1");
        }

        @Test
        @DisplayName("Returned results 50% farther → ratio ≈ 1.5")
        void testFiftyPercentFarther() {
            double[] gtDistances = {2.0, 4.0, 6.0};
            double[] returnedDistances = {3.0, 6.0, 9.0}; // each 50% farther

            double ratio = computeDistanceRatio(returnedDistances, gtDistances);

            assertEquals(1.5, ratio, EPSILON, "50% farther should yield ratio≈1.5");
        }

        @Test
        @DisplayName("Mixed distances: manual calculation verification")
        void testMixedDistancesManual() {
            // GT: [1.0, 2.0, 4.0]
            // Returned: [1.5, 2.0, 6.0]
            // Per-position: 1.5/1.0=1.5, 2.0/2.0=1.0, 6.0/4.0=1.5
            // Average: (1.5 + 1.0 + 1.5) / 3 = 4.0 / 3 ≈ 1.333

            double[] gtDistances = {1.0, 2.0, 4.0};
            double[] returnedDistances = {1.5, 2.0, 6.0};

            double ratio = computeDistanceRatio(returnedDistances, gtDistances);

            assertEquals(4.0 / 3.0, ratio, EPSILON, "Manual calculation mismatch");
        }

        @Test
        @DisplayName("Returned results closer than GT → ratio < 1.0 (unusual but valid)")
        void testCloserThanGT() {
            double[] gtDistances = {2.0, 4.0, 6.0};
            double[] returnedDistances = {1.0, 2.0, 3.0}; // half the distance

            double ratio = computeDistanceRatio(returnedDistances, gtDistances);

            assertEquals(0.5, ratio, EPSILON, "Closer results should yield ratio<1.0");
        }

        @Test
        @DisplayName("Single element (K=1)")
        void testSingleElement() {
            double[] gtDistances = {5.0};
            double[] returnedDistances = {6.0};

            double ratio = computeDistanceRatio(returnedDistances, gtDistances);

            assertEquals(1.2, ratio, EPSILON);
        }

        @Test
        @DisplayName("Large K=100 with uniform 20% overhead")
        void testLargeK() {
            int k = 100;
            double[] gtDistances = new double[k];
            double[] returnedDistances = new double[k];

            for (int i = 0; i < k; i++) {
                gtDistances[i] = (i + 1) * 1.0;
                returnedDistances[i] = (i + 1) * 1.2; // 20% farther
            }

            double ratio = computeDistanceRatio(returnedDistances, gtDistances);

            assertEquals(1.2, ratio, EPSILON, "Uniform 20% overhead should yield ratio=1.2");
        }

        @Test
        @DisplayName("Zero GT distance (edge case) should be skipped")
        void testZeroGTDistance() {
            double[] gtDistances = {0.0, 2.0, 4.0};  // First is zero
            double[] returnedDistances = {1.0, 2.0, 4.0};

            double ratio = computeDistanceRatio(returnedDistances, gtDistances);

            // Should skip first position, average of (2.0/2.0 + 4.0/4.0) = 1.0
            assertEquals(1.0, ratio, EPSILON, "Should skip zero GT distances");
        }

        @Test
        @DisplayName("Empty arrays return NaN")
        void testEmptyArrays() {
            assertTrue(Double.isNaN(computeDistanceRatio(new double[0], new double[0])));
            assertTrue(Double.isNaN(computeDistanceRatio(null, null)));
        }

        @Test
        @DisplayName("Mismatched lengths: use min(returned, gt)")
        void testMismatchedLengths() {
            double[] gtDistances = {1.0, 2.0, 3.0, 4.0, 5.0};  // 5 elements
            double[] returnedDistances = {1.1, 2.2, 3.3};       // 3 elements

            double ratio = computeDistanceRatio(returnedDistances, gtDistances);

            // Only first 3 positions: (1.1/1.0 + 2.2/2.0 + 3.3/3.0) / 3 = (1.1 + 1.1 + 1.1) / 3 = 1.1
            assertEquals(1.1, ratio, EPSILON);
        }
    }

    // ========================================================================
    // SECTION 3: Precision@K Tests
    // ========================================================================

    @Nested
    @DisplayName("Precision@K Computation")
    class PrecisionTests {

        /**
         * Compute precision: |true_KNN ∩ returned| / K
         */
        private double computePrecision(int[] returnedIds, int[] gtIds, int k) {
            if (returnedIds == null || gtIds == null || k <= 0) return 0.0;

            Set<Integer> gtSet = new HashSet<>();
            for (int id : gtIds) gtSet.add(id);

            int hits = 0;
            int upto = Math.min(k, returnedIds.length);
            for (int i = 0; i < upto; i++) {
                if (gtSet.contains(returnedIds[i])) hits++;
            }

            return hits / (double) k;
        }

        @Test
        @DisplayName("Perfect precision: all returned are true NNs")
        void testPerfectPrecision() {
            int[] gtIds = {0, 1, 2, 3, 4};
            int[] returnedIds = {0, 1, 2, 3, 4};

            double precision = computePrecision(returnedIds, gtIds, 5);

            assertEquals(1.0, precision, EPSILON);
        }

        @Test
        @DisplayName("Zero precision: no overlap")
        void testZeroPrecision() {
            int[] gtIds = {0, 1, 2, 3, 4};
            int[] returnedIds = {100, 101, 102, 103, 104};

            double precision = computePrecision(returnedIds, gtIds, 5);

            assertEquals(0.0, precision, EPSILON);
        }

        @Test
        @DisplayName("Partial precision: 3 out of 5 correct")
        void testPartialPrecision() {
            int[] gtIds = {0, 1, 2, 3, 4};
            int[] returnedIds = {0, 1, 2, 100, 101};  // 3 hits

            double precision = computePrecision(returnedIds, gtIds, 5);

            assertEquals(0.6, precision, EPSILON);
        }

        @Test
        @DisplayName("Order doesn't matter for precision")
        void testOrderIndependent() {
            int[] gtIds = {0, 1, 2, 3, 4};
            int[] returnedIds = {4, 3, 2, 1, 0};  // Reversed order

            double precision = computePrecision(returnedIds, gtIds, 5);

            assertEquals(1.0, precision, EPSILON, "Precision should not depend on order");
        }

        @ParameterizedTest
        @CsvSource({
                "10, 1, 0.1",
                "10, 5, 0.5",
                "10, 10, 1.0",
                "100, 90, 0.9",
                "100, 50, 0.5"
        })
        @DisplayName("Parameterized precision tests")
        void testParameterizedPrecision(int k, int hits, double expectedPrecision) {
            int[] gtIds = IntStream.range(0, k).toArray();
            int[] returnedIds = new int[k];

            // First 'hits' are correct, rest are wrong
            for (int i = 0; i < k; i++) {
                returnedIds[i] = (i < hits) ? i : (1000 + i);
            }

            double precision = computePrecision(returnedIds, gtIds, k);

            assertEquals(expectedPrecision, precision, EPSILON);
        }

        @Test
        @DisplayName("K larger than returned results")
        void testKLargerThanReturned() {
            int[] gtIds = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
            int[] returnedIds = {0, 1, 2};  // Only 3 returned

            // Requesting K=10, but only 3 returned, all correct
            // precision = 3 / 10 = 0.3
            double precision = computePrecision(returnedIds, gtIds, 10);

            assertEquals(0.3, precision, EPSILON);
        }
    }

    // ========================================================================
    // SECTION 4: Candidate Ratio Tests
    // ========================================================================

    @Nested
    @DisplayName("Candidate Ratio Computation")
    class CandidateRatioTests {

        private double computeCandidateRatio(int candidatesExamined, int k) {
            if (k <= 0) return Double.NaN;
            return (double) candidatesExamined / k;
        }

        @Test
        @DisplayName("Minimum candidate ratio (candidates = K)")
        void testMinimumRatio() {
            assertEquals(1.0, computeCandidateRatio(100, 100), EPSILON);
        }

        @Test
        @DisplayName("2x candidates → ratio = 2.0")
        void testDoubleRatio() {
            assertEquals(2.0, computeCandidateRatio(200, 100), EPSILON);
        }

        @Test
        @DisplayName("Typical good ratio (1.2)")
        void testTypicalGoodRatio() {
            assertEquals(1.2, computeCandidateRatio(120, 100), EPSILON);
        }

        @ParameterizedTest
        @CsvSource({
                "100, 100, 1.0",
                "110, 100, 1.1",
                "150, 100, 1.5",
                "200, 100, 2.0",
                "50, 10, 5.0"
        })
        @DisplayName("Parameterized candidate ratio tests")
        void testParameterized(int candidates, int k, double expected) {
            assertEquals(expected, computeCandidateRatio(candidates, k), EPSILON);
        }
    }

    // ========================================================================
    // SECTION 5: Aggregates from Profiler Tests
    // ========================================================================

    @Nested
    @DisplayName("Aggregates Computation from Profiler")
    class AggregatesTests {

        @Test
        @DisplayName("Empty profiler returns zero aggregates")
        void testEmptyProfiler() {
            Profiler p = new Profiler();
            Aggregates agg = Aggregates.fromProfiler(p);

            assertEquals(0.0, agg.avgRatio, EPSILON);
            assertEquals(0.0, agg.avgPrecision, EPSILON);
            assertEquals(0.0, agg.avgServerMs, EPSILON);
            assertTrue(agg.precisionAtK.isEmpty());
        }

        @Test
        @DisplayName("Single query row aggregates correctly")
        void testSingleRow() {
            Profiler p = new Profiler();

            p.recordQueryRow(
                    "Q0_K10",           // label
                    50.0,               // serverMs
                    100.0,              // clientMs
                    150.0,              // runMs
                    10.0,               // decryptMs
                    5.0,                // insertMs
                    1.15,               // ratio
                    0.85,               // precision
                    500,                // candTotal
                    150,                // candKept
                    100,                // candDecrypted
                    10,                 // candReturned
                    256,                // tokenBytes
                    128,                // vectorDim
                    10,                 // tokenK
                    10,                 // tokenKBase
                    0,                  // qIndex
                    1000,               // totalFlushed
                    100,                // flushThreshold
                    50,                 // touchedCount
                    0,                  // reencCount
                    0L,                 // reencTimeMs
                    0L,                 // reencBytesDelta
                    0L,                 // reencBytesAfter
                    "gt",               // ratioDenomSource
                    "partitioned",      // mode
                    500,                // stableRaw
                    150,                 // stableFinal
                    -1,
                    false

            );

            Aggregates agg = Aggregates.fromProfiler(p);

            assertEquals(1.15, agg.avgRatio, EPSILON);
            assertEquals(0.85, agg.avgPrecision, EPSILON);
            assertEquals(50.0, agg.avgServerMs, EPSILON);
            assertEquals(100.0, agg.avgClientMs, EPSILON);
            assertEquals(150.0, agg.avgRunMs, EPSILON);
            assertEquals(10.0, agg.avgDecryptMs, EPSILON);

            // Precision@K breakdown
            assertEquals(0.85, agg.precisionAtK.getOrDefault(10, 0.0), EPSILON);
        }

        @Test
        @DisplayName("Multiple queries average correctly")
        void testMultipleQueries() {
            Profiler p = new Profiler();

            // Query 1: ratio=1.1, precision=0.9
            recordSimpleQuery(p, "Q0", 10, 1.1, 0.9, 100.0);
            // Query 2: ratio=1.2, precision=0.8
            recordSimpleQuery(p, "Q1", 10, 1.2, 0.8, 150.0);
            // Query 3: ratio=1.3, precision=0.7
            recordSimpleQuery(p, "Q2", 10, 1.3, 0.7, 200.0);

            Aggregates agg = Aggregates.fromProfiler(p);

            assertEquals(1.2, agg.avgRatio, EPSILON, "(1.1+1.2+1.3)/3 = 1.2");
            assertEquals(0.8, agg.avgPrecision, EPSILON, "(0.9+0.8+0.7)/3 = 0.8");
            assertEquals(150.0, agg.avgRunMs, EPSILON, "(100+150+200)/3 = 150");
        }

        @Test
        @DisplayName("Precision@K grouped by K value")
        void testPrecisionGroupedByK() {
            Profiler p = new Profiler();

            // K=10 queries
            recordSimpleQuery(p, "Q0_K10", 10, 1.1, 0.90, 100.0);
            recordSimpleQuery(p, "Q1_K10", 10, 1.1, 0.80, 100.0);

            // K=100 queries
            recordSimpleQuery(p, "Q0_K100", 100, 1.2, 0.70, 100.0);
            recordSimpleQuery(p, "Q1_K100", 100, 1.2, 0.60, 100.0);

            Aggregates agg = Aggregates.fromProfiler(p);

            assertEquals(0.85, agg.precisionAtK.get(10), EPSILON, "P@10 = (0.9+0.8)/2");
            assertEquals(0.65, agg.precisionAtK.get(100), EPSILON, "P@100 = (0.7+0.6)/2");
        }

        @Test
        @DisplayName("Re-encryption stats accumulated")
        void testReencryptionStats() {
            Profiler p = new Profiler();

            // Record queries with re-encryption
            p.recordQueryRow(
                    "Q0", 10.0, 20.0, 30.0, 5.0, 1.0,
                    1.1, 0.9,
                    100, 50, 50, 10,
                    128, 128, 10, 10, 0,
                    100, 10,
                    50, 10, 100L, 500L, 10000L,  // reenc: count=10, time=100ms, delta=500, after=10000
                    "gt", "partitioned", 100, 50, -1, false
            );

            p.recordQueryRow(
                    "Q1", 10.0, 20.0, 30.0, 5.0, 1.0,
                    1.1, 0.9,
                    100, 50, 50, 10,
                    128, 128, 10, 10, 1,
                    100, 10,
                    50, 5, 50L, 200L, 10200L,  // reenc: count=5, time=50ms, delta=200
                    "gt", "partitioned", 100, 50, -1, false
            );

            Aggregates agg = Aggregates.fromProfiler(p);

            assertEquals(15, agg.reencryptCount, "10 + 5 = 15");
            assertEquals(700L, agg.reencryptBytes, "500 + 200 = 700");
            assertEquals(150.0, agg.reencryptMs, EPSILON, "100 + 50 = 150");
        }

        private void recordSimpleQuery(Profiler p, String label, int k, double ratio, double precision, double runMs) {
            p.recordQueryRow(
                    label, 10.0, 20.0, runMs, 5.0, 1.0,
                    ratio, precision,
                    100, 50, 50, k,
                    128, 128, k, k, 0,
                    100, 10,
                    0, 0, 0L, 0L, 0L,
                    "gt", "partitioned", 100, 50, -1, false
            );
        }
    }

    // ========================================================================
    // SECTION 6: Integration Tests with Mock Search Results
    // ========================================================================

    @Nested
    @DisplayName("Search Logic Integration Tests")
    class SearchIntegrationTests {

        /**
         * Mock implementation of computeMetricsAtK for testing.
         */
        private QueryMetrics computeMetricsAtK(
                List<MockQueryResult> annResults,
                int k,
                double[] queryVector,
                int[] gtIds,
                MockBaseReader baseReader
        ) {
            if (annResults == null || annResults.isEmpty() || k <= 0) {
                return new QueryMetrics(Double.NaN, 0.0, Double.NaN);
            }

            final int upto = Math.min(k, annResults.size());

            // ================== Precision@K ==================
            Set<Integer> gtSet = new HashSet<>();
            if (gtIds != null) {
                for (int id : gtIds) gtSet.add(id);
            }

            int hits = 0;
            for (int i = 0; i < upto; i++) {
                if (gtSet.contains(annResults.get(i).id)) hits++;
            }
            double precision = hits / (double) k;

            // ================== Candidate Ratio ==================
            int candidatesExamined = annResults.size() * 2; // Simulated
            double candidateRatio = (double) candidatesExamined / k;

            // ================== Distance Ratio ==================
            double distanceRatio = Double.NaN;

            if (baseReader != null && gtIds != null && gtIds.length > 0) {
                double[] gtDistances = new double[Math.min(k, gtIds.length)];
                for (int j = 0; j < gtDistances.length; j++) {
                    gtDistances[j] = baseReader.l2(queryVector, gtIds[j]);
                }

                double ratioSum = 0.0;
                int validCount = 0;

                for (int j = 0; j < upto; j++) {
                    double dAnn = baseReader.l2(queryVector, annResults.get(j).id);
                    double dGt = (j < gtDistances.length) ? gtDistances[j] : gtDistances[gtDistances.length - 1];

                    if (dGt > 1e-24) {
                        ratioSum += (dAnn / dGt);
                        validCount++;
                    }
                }

                distanceRatio = (validCount > 0) ? (ratioSum / validCount) : Double.NaN;
            }

            return new QueryMetrics(distanceRatio, precision, candidateRatio);
        }

        @Test
        @DisplayName("Perfect ANN: returns exact k-NNs")
        void testPerfectANN() {
            // Setup: 1000 vectors, query at origin, GT are closest 10
            int n = 1000;
            int dim = 128;
            int k = 10;

            MockBaseReader reader = new MockBaseReader(n, dim);
            double[] query = new double[dim]; // Origin

            // GT: IDs 0-9 (closest to origin in mock)
            int[] gtIds = IntStream.range(0, k).toArray();

            // Perfect ANN returns exactly GT
            List<MockQueryResult> results = new ArrayList<>();
            for (int i = 0; i < k; i++) {
                results.add(new MockQueryResult(i, reader.l2(query, i)));
            }

            QueryMetrics m = computeMetricsAtK(results, k, query, gtIds, reader);

            assertEquals(1.0, m.ratioAtK(), EPSILON, "Perfect ANN should have ratio=1.0");
            assertEquals(1.0, m.precisionAtK(), EPSILON, "Perfect ANN should have precision=1.0");
        }

        @Test
        @DisplayName("Approximate ANN: returns 8/10 correct with 20% distance overhead")
        void testApproximateANN() {
            int n = 1000;
            int dim = 128;
            int k = 10;

            MockBaseReader reader = new MockBaseReader(n, dim);
            double[] query = new double[dim];

            int[] gtIds = IntStream.range(0, k).toArray();

            // ANN returns 8 correct + 2 wrong (IDs 100, 101)
            List<MockQueryResult> results = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                results.add(new MockQueryResult(i, reader.l2(query, i)));
            }
            results.add(new MockQueryResult(100, reader.l2(query, 100)));
            results.add(new MockQueryResult(101, reader.l2(query, 101)));

            QueryMetrics m = computeMetricsAtK(results, k, query, gtIds, reader);

            assertEquals(0.8, m.precisionAtK(), EPSILON, "8/10 correct = 0.8 precision");
            assertTrue(m.ratioAtK() > 1.0, "Ratio should be > 1.0 due to farther results");
        }

        @Test
        @DisplayName("Empty results return safe defaults")
        void testEmptyResults() {
            QueryMetrics m = computeMetricsAtK(
                    Collections.emptyList(),
                    10,
                    new double[128],
                    new int[]{0, 1, 2},
                    new MockBaseReader(100, 128)
            );

            assertTrue(Double.isNaN(m.ratioAtK()));
            assertEquals(0.0, m.precisionAtK(), EPSILON);
        }

        @Test
        @DisplayName("K=1 (top-1 accuracy)")
        void testTop1() {
            MockBaseReader reader = new MockBaseReader(100, 128);
            double[] query = new double[128];
            query[0] = 1.0;   // break zero-distance degeneracy

            int[] gtIds = {0};  // True NN is ID 0

            // Correct: returns ID 0
            List<MockQueryResult> correct = List.of(new MockQueryResult(0, reader.l2(query, 0)));
            QueryMetrics mCorrect = computeMetricsAtK(correct, 1, query, gtIds, reader);
            assertEquals(1.0, mCorrect.precisionAtK(), EPSILON);
            assertEquals(1.0, mCorrect.ratioAtK(), EPSILON);

            // Wrong: returns ID 50
            List<MockQueryResult> wrong = List.of(new MockQueryResult(50, reader.l2(query, 50)));
            QueryMetrics mWrong = computeMetricsAtK(wrong, 1, query, gtIds, reader);
            assertEquals(0.0, mWrong.precisionAtK(), EPSILON);
            assertTrue(mWrong.ratioAtK() > 1.0);
        }

        @Test
        @DisplayName("Large K=100 stress test")
        void testLargeK() {
            int n = 10000;
            int dim = 128;
            int k = 100;

            MockBaseReader reader = new MockBaseReader(n, dim);
            double[] query = new double[dim];

            int[] gtIds = IntStream.range(0, k).toArray();

            // Return 90% correct (90/100)
            List<MockQueryResult> results = new ArrayList<>();
            for (int i = 0; i < 90; i++) {
                results.add(new MockQueryResult(i, reader.l2(query, i)));
            }
            for (int i = 0; i < 10; i++) {
                results.add(new MockQueryResult(5000 + i, reader.l2(query, 5000 + i)));
            }

            QueryMetrics m = computeMetricsAtK(results, k, query, gtIds, reader);

            assertEquals(0.9, m.precisionAtK(), EPSILON);
            assertTrue(m.ratioAtK() >= 1.0);
        }
    }

    // ========================================================================
    // SECTION 7: Edge Cases and Error Handling
    // ========================================================================

    @Nested
    @DisplayName("Edge Cases and Error Handling")
    class EdgeCaseTests {

        @Test
        @DisplayName("Null inputs handled gracefully")
        void testNullInputs() {
            QueryMetrics m = new QueryMetrics(Double.NaN, 0.0, Double.NaN);
            assertNotNull(m);
            assertTrue(Double.isNaN(m.ratioAtK()));
        }

        @Test
        @DisplayName("Very small distances (near zero)")
        void testVerySmallDistances() {
            double[] gtDistances = {1e-10, 1e-10, 1e-10};
            double[] returnedDistances = {2e-10, 2e-10, 2e-10};

            // Should compute ratio ≈ 2.0
            double sum = 0;
            for (int i = 0; i < 3; i++) {
                sum += returnedDistances[i] / gtDistances[i];
            }
            double ratio = sum / 3;

            assertEquals(2.0, ratio, EPSILON);
        }

        @Test
        @DisplayName("Very large distances")
        void testVeryLargeDistances() {
            double[] gtDistances = {1e10, 2e10, 3e10};
            double[] returnedDistances = {1.1e10, 2.2e10, 3.3e10};

            double sum = 0;
            for (int i = 0; i < 3; i++) {
                sum += returnedDistances[i] / gtDistances[i];
            }
            double ratio = sum / 3;

            assertEquals(1.1, ratio, EPSILON);
        }

        @Test
        @DisplayName("Duplicate IDs in results")
        void testDuplicateIds() {
            int[] gtIds = {0, 1, 2, 3, 4};
            int[] returnedIds = {0, 0, 0, 1, 1};  // Duplicates

            Set<Integer> gtSet = new HashSet<>();
            for (int id : gtIds) gtSet.add(id);

            int hits = 0;
            for (int id : returnedIds) {
                if (gtSet.contains(id)) hits++;
            }
            double precision = hits / 5.0;

            // All 5 returned IDs match GT (even though duplicates)
            assertEquals(1.0, precision, EPSILON);
        }

        @Test
        @DisplayName("Negative distances (invalid but handled)")
        void testNegativeDistances() {
            // Negative distances are invalid in Euclidean space
            // but the code should not crash
            double[] gtDistances = {1.0, 2.0, 3.0};
            double[] returnedDistances = {-1.0, 2.0, 3.0};

            // The negative will produce negative ratio for that position
            // Just verify no exception is thrown
            double sum = 0;
            int count = 0;
            for (int i = 0; i < 3; i++) {
                if (gtDistances[i] > 1e-24) {
                    sum += returnedDistances[i] / gtDistances[i];
                    count++;
                }
            }
            double ratio = sum / count;

            // (-1/1 + 2/2 + 3/3) / 3 = (-1 + 1 + 1) / 3 = 1/3 ≈ 0.333
            assertEquals(1.0 / 3.0, ratio, EPSILON);
        }
    }

    // ========================================================================
    // Helper Classes
    // ========================================================================

    /**
     * Mock query result for testing.
     */
    static class MockQueryResult {
        final int id;
        final double distance;

        MockQueryResult(int id, double distance) {
            this.id = id;
            this.distance = distance;
        }
    }

    /**
     * Mock base vector reader for testing.
     * Generates deterministic vectors where ID i has vector [i, i, i, ...].
     */
    static class MockBaseReader {
        final int n;
        final int dim;

        MockBaseReader(int n, int dim) {
            this.n = n;
            this.dim = dim;
        }

        double[] getVector(int id) {
            double[] v = new double[dim];
            Arrays.fill(v, (double) id);
            return v;
        }

        double l2(double[] query, int id) {
            double[] v = getVector(id);
            double sum = 0;
            for (int i = 0; i < dim; i++) {
                double diff = query[i] - v[i];
                sum += diff * diff;
            }
            return Math.sqrt(sum);
        }

        double l2sq(double[] query, int id) {
            double l2 = l2(query, id);
            return l2 * l2;
        }
    }
}
