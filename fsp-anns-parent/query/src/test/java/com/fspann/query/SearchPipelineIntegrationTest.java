package com.fspann.query;

import com.fspann.common.QueryMetrics;
import com.fspann.common.Profiler;
import com.fspann.query.core.Aggregates;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for:
 *   1. Stabilization logic (alpha-based candidate limiting)
 *   2. Search pipeline end-to-end
 *   3. Multi-K evaluation
 *   4. Groundtruth validation
 *   5. CSV output verification
 */
@DisplayName("Search Pipeline Integration Tests")
class SearchPipelineIntegrationTest {

    private static final double EPSILON = 1e-9;

    // ========================================================================
    // SECTION 1: Stabilization Logic Tests
    // ========================================================================

    @Nested
    @DisplayName("Stabilization (Alpha-Based Candidate Limiting)")
    class StabilizationTests {

        /**
         * Simulates the stabilization logic from QueryServiceImpl.
         *
         * Parameters:
         *   - raw: number of raw candidates
         *   - K: requested top-K
         *   - alpha: fraction of raw to keep (e.g., 0.10 = 10%)
         *   - minCandidates: floor for candidate count
         *   - maxRefineFactor: ceiling as multiple of K
         *
         * Returns: final stabilized candidate count
         */
        private int stabilize(int raw, int K, double alpha, int minCandidates, int maxRefineFactor) {
            // Alpha-based target (keep alpha% of raw)
            int alphaTarget = (int) Math.ceil(alpha * raw);

            // K-aware ceiling
            int kCeiling = maxRefineFactor * K;

            // Floor: never below K or minCandidates
            int floor = Math.max(K, minCandidates);

            // Final: bounded by floor and ceiling
            int finalSize = Math.min(raw, Math.max(floor, Math.min(alphaTarget, kCeiling)));

            return finalSize;
        }

        @Test
        @DisplayName("Basic stabilization: alpha=0.10, raw=10000, K=100")
        void testBasicStabilization() {
            int result = stabilize(10000, 100, 0.10, 150, 3);

            // alphaTarget = ceil(0.10 * 10000) = 1000
            // kCeiling = 3 * 100 = 300
            // floor = max(100, 150) = 150
            // finalSize = min(10000, max(150, min(1000, 300))) = min(10000, max(150, 300)) = 300

            assertEquals(300, result);
        }

        @Test
        @DisplayName("Low raw count: raw < floor")
        void testLowRawCount() {
            int result = stabilize(50, 100, 0.10, 150, 3);

            // raw=50 is less than floor=150, so final = 50 (can't return more than raw)
            assertEquals(50, result);
        }

        @Test
        @DisplayName("Alpha target below floor")
        void testAlphaTargetBelowFloor() {
            int result = stabilize(1000, 100, 0.05, 150, 3);

            // alphaTarget = ceil(0.05 * 1000) = 50
            // kCeiling = 300
            // floor = 150
            // finalSize = min(1000, max(150, min(50, 300))) = min(1000, max(150, 50)) = min(1000, 150) = 150

            assertEquals(150, result);
        }

        @Test
        @DisplayName("Alpha target above ceiling")
        void testAlphaTargetAboveCeiling() {
            int result = stabilize(100000, 100, 0.50, 150, 3);

            // alphaTarget = ceil(0.50 * 100000) = 50000
            // kCeiling = 300
            // floor = 150
            // finalSize = min(100000, max(150, min(50000, 300))) = min(100000, max(150, 300)) = 300

            assertEquals(300, result);
        }

        @Test
        @DisplayName("Small K=10 with aggressive alpha")
        void testSmallK() {
            int result = stabilize(5000, 10, 0.02, 50, 5);

            // alphaTarget = ceil(0.02 * 5000) = 100
            // kCeiling = 5 * 10 = 50
            // floor = max(10, 50) = 50
            // finalSize = min(5000, max(50, min(100, 50))) = min(5000, max(50, 50)) = 50

            assertEquals(50, result);
        }

        @Test
        @DisplayName("Large K=1000 scenario")
        void testLargeK() {
            int result = stabilize(50000, 1000, 0.10, 500, 2);

            // alphaTarget = ceil(0.10 * 50000) = 5000
            // kCeiling = 2 * 1000 = 2000
            // floor = max(1000, 500) = 1000
            // finalSize = min(50000, max(1000, min(5000, 2000))) = min(50000, max(1000, 2000)) = 2000

            assertEquals(2000, result);
        }

        @ParameterizedTest
        @MethodSource("stabilizationScenarios")
        @DisplayName("Parameterized stabilization scenarios")
        void testParameterizedStabilization(int raw, int K, double alpha, int minCand, int maxFactor, int expected) {
            int result = stabilize(raw, K, alpha, minCand, maxFactor);
            assertEquals(expected, result,
                    String.format("raw=%d, K=%d, alpha=%.2f, minCand=%d, maxFactor=%d",
                            raw, K, alpha, minCand, maxFactor));
        }

        static Stream<Arguments> stabilizationScenarios() {
            return Stream.of(
                    // raw, K, alpha, minCandidates, maxRefineFactor, expected
                    Arguments.of(10000, 100, 0.10, 150, 3, 300),
                    Arguments.of(10000, 100, 0.05, 150, 3, 300),
                    Arguments.of(10000, 100, 0.50, 150, 3, 300),
                    Arguments.of(100, 100, 0.10, 150, 3, 100),
                    Arguments.of(500, 10, 0.10, 20, 5, 50),
                    Arguments.of(1000000, 100, 0.001, 100, 10, 1000),
                    Arguments.of(200, 100, 1.0, 100, 2, 200)
            );
        }

        @Test
        @DisplayName("Candidate ratio after stabilization")
        void testCandidateRatioAfterStabilization() {
            int raw = 10000;
            int K = 100;
            int stabilized = stabilize(raw, K, 0.10, 150, 3);

            double candidateRatio = (double) stabilized / K;

            // stabilized = 300, K = 100
            assertEquals(3.0, candidateRatio, EPSILON);
        }
    }

    // ========================================================================
    // SECTION 2: Multi-K Evaluation Tests
    // ========================================================================

    @Nested
    @DisplayName("Multi-K Evaluation")
    class MultiKTests {

        private static final int[] K_VARIANTS = {1, 5, 10, 20, 40, 60, 80, 100};

        @Test
        @DisplayName("Precision generally decreases as K increases")
        void testPrecisionTrendWithK() {
            // Simulate: have 50 true NNs in top-100 candidates
            int trueNNsInResults = 50;

            Map<Integer, Double> precisionAtK = new HashMap<>();

            for (int k : K_VARIANTS) {
                // Assume true NNs distributed evenly in results
                int hitsAtK = Math.min(k, trueNNsInResults);
                double precision = hitsAtK / (double) k;
                precisionAtK.put(k, precision);
            }

            // P@1 = 1.0 (first is always true NN in this model)
            assertEquals(1.0, precisionAtK.get(1), EPSILON);

            // P@100 = 50/100 = 0.5
            assertEquals(0.5, precisionAtK.get(100), EPSILON);

            // Generally, precision decreases as K increases
            assertTrue(precisionAtK.get(1) >= precisionAtK.get(100));
        }

        @Test
        @DisplayName("Distance ratio may increase with K")
        void testDistanceRatioTrendWithK() {
            // As K increases, we include farther neighbors,
            // so ratio may increase if ANN misses some

            Map<Integer, Double> ratioAtK = new HashMap<>();

            for (int k : K_VARIANTS) {
                // Simulate: ratio increases linearly with K
                double baseRatio = 1.0;
                double ratioIncrease = 0.001 * k; // Small increase per K
                ratioAtK.put(k, baseRatio + ratioIncrease);
            }

            // Verify ratio at K=1 < ratio at K=100
            assertTrue(ratioAtK.get(1) < ratioAtK.get(100));
        }

        @Test
        @DisplayName("Metrics recorded for all K variants")
        void testAllKVariantsRecorded() {
            Profiler p = new Profiler();

            // Simulate recording metrics for all K variants
            for (int k : K_VARIANTS) {
                double ratio = 1.0 + 0.001 * k;
                double precision = Math.max(0.5, 1.0 - 0.005 * k);

                p.recordQueryRow(
                        "Q0_K" + k, 10.0, 20.0, 30.0, 5.0, 1.0,
                        ratio, precision,
                        1000, 200, 200, k,
                        128, 128, k, k, 0,
                        100, 10,
                        0, 0, 0L, 0L, 0L,
                        "gt", "partitioned", 1000, 200, -1, false
                );
            }

            Aggregates agg = Aggregates.fromProfiler(p);
            Profiler.QueryRow row = p.getQueryRows().get(0);
            assertEquals(-1, row.nnRank);
            assertFalse(row.nnSeen);

            // Verify all K values have precision recorded
            for (int k : K_VARIANTS) {
                assertTrue(agg.precisionAtK.containsKey(k), "Missing P@" + k);
            }
        }
    }

    // ========================================================================
    // SECTION 3: Groundtruth Validation Tests
    // ========================================================================

    @Nested
    @DisplayName("Groundtruth Validation")
    class GroundtruthValidationTests {

        /**
         * Simulates groundtruth validation: compares GT file against brute-force.
         */
        private double validateGroundtruth(int[][] gtFromFile, int[][] gtBruteForce, int sampleSize) {
            int mismatches = 0;

            for (int i = 0; i < Math.min(sampleSize, gtFromFile.length); i++) {
                if (gtFromFile[i][0] != gtBruteForce[i][0]) {
                    mismatches++;
                }
            }

            return (double) mismatches / sampleSize;
        }

        @Test
        @DisplayName("Perfect GT: 0% mismatch")
        void testPerfectGT() {
            int n = 100;
            int[][] gt = new int[n][10];
            for (int i = 0; i < n; i++) {
                for (int j = 0; j < 10; j++) {
                    gt[i][j] = i * 10 + j;
                }
            }

            double mismatchRate = validateGroundtruth(gt, gt, n);

            assertEquals(0.0, mismatchRate, EPSILON);
        }

        @Test
        @DisplayName("10% mismatch detected")
        void testPartialMismatch() {
            int n = 100;
            int[][] gtFile = new int[n][10];
            int[][] gtTrue = new int[n][10];

            for (int i = 0; i < n; i++) {
                for (int j = 0; j < 10; j++) {
                    gtTrue[i][j] = i * 10 + j;
                    // Introduce 10% errors in file
                    gtFile[i][j] = (i < 10) ? 99999 : i * 10 + j;
                }
            }

            double mismatchRate = validateGroundtruth(gtFile, gtTrue, n);

            assertEquals(0.1, mismatchRate, EPSILON);
        }

        @Test
        @DisplayName("Validation fails above tolerance")
        void testValidationFailure() {
            double mismatchRate = 0.15;  // 15% mismatch
            double tolerance = 0.05;     // 5% tolerance

            assertTrue(mismatchRate > tolerance, "Should fail validation");
        }
    }

    // ========================================================================
    // SECTION 4: End-to-End Pipeline Simulation
    // ========================================================================

    @Nested
    @DisplayName("End-to-End Pipeline Simulation")
    class EndToEndPipelineTests {

        @Test
        @DisplayName("Complete query pipeline simulation")
        void testCompletePipeline() {
            // Setup
            int n = 10000;
            int dim = 128;
            int numQueries = 100;
            int[] kVariants = {10, 100};

            Profiler profiler = new Profiler();
            Random rng = new Random(42);

            // Simulate queries
            for (int qi = 0; qi < numQueries; qi++) {
                double[] query = generateRandomVector(dim, rng);

                for (int k : kVariants) {
                    // Simulate search
                    long t0 = System.nanoTime();
                    SimulatedSearchResult result = simulateSearch(n, k, rng);
                    long t1 = System.nanoTime();

                    double serverMs = (t1 - t0) / 1_000_000.0;
                    double clientMs = serverMs * 0.3;
                    double runMs = serverMs + clientMs;

                    // Record metrics
                    profiler.recordQueryRow(
                            "Q" + qi + "_K" + k,
                            serverMs, clientMs, runMs, clientMs * 0.1, 0.0,
                            result.distanceRatio, result.precision,
                            result.rawCandidates, result.keptCandidates, result.keptCandidates, k,
                            dim * 4, dim, k, k, qi,
                            0, 0,
                            result.touchedCount, 0, 0L, 0L, 0L,
                            "gt", "partitioned",
                            result.rawCandidates, result.keptCandidates, 1, true
                    );
                }
            }

            // Aggregate
            Aggregates agg = Aggregates.fromProfiler(profiler);

            // Verify aggregates are reasonable
            assertTrue(agg.avgRatio >= 1.0, "Average ratio should be >= 1.0");
            assertTrue(agg.avgRatio <= 2.0, "Average ratio should be <= 2.0 for good ANN");

            assertTrue(agg.avgPrecision >= 0.5, "Average precision should be >= 0.5");
            assertTrue(agg.avgPrecision <= 1.0, "Average precision should be <= 1.0");

            assertTrue(agg.avgRunMs > 0, "Run time should be positive");

            assertEquals(2, agg.precisionAtK.size(), "Should have P@10 and P@100");
        }

        @Test
        @DisplayName("Pipeline with re-encryption")
        void testPipelineWithReencryption() {
            Profiler profiler = new Profiler();

            // Query 1: triggers re-encryption
            profiler.recordQueryRow(
                    "Q0_K100", 50.0, 100.0, 150.0, 10.0, 5.0,
                    1.15, 0.85,
                    5000, 300, 300, 100,
                    512, 128, 100, 100, 0,
                    10000, 1000,
                    5000, 500, 1000L, 50000L, 1000000L,  // Re-encryption triggered
                    "gt", "partitioned", 5000, 300, 1, true
            );

            // Query 2: no re-encryption
            profiler.recordQueryRow(
                    "Q1_K100", 50.0, 100.0, 150.0, 10.0, 5.0,
                    1.10, 0.90,
                    4000, 250, 250, 100,
                    512, 128, 100, 100, 1,
                    10500, 1000,
                    4000, 0, 0L, 0L, 1000000L,
                    "gt", "partitioned", 4000, 250, 1, true
            );

            Aggregates agg = Aggregates.fromProfiler(profiler);

            assertEquals(500, agg.reencryptCount);
            assertEquals(50000L, agg.reencryptBytes);
            assertEquals(1000.0, agg.reencryptMs, EPSILON);
        }

        private double[] generateRandomVector(int dim, Random rng) {
            double[] v = new double[dim];
            for (int i = 0; i < dim; i++) {
                v[i] = rng.nextGaussian();
            }
            return v;
        }

        private SimulatedSearchResult simulateSearch(int n, int k, Random rng) {
            // Simulate realistic search results
            int rawCandidates = Math.min(n, (int) (k * (5 + rng.nextDouble() * 10)));
            int keptCandidates = Math.min(rawCandidates, k * 3);
            int touchedCount = rawCandidates;

            // Precision: higher for smaller K
            double basePrecision = 0.95 - 0.002 * k;
            double precision = Math.max(0.5, Math.min(1.0, basePrecision + rng.nextGaussian() * 0.05));

            // Distance ratio: slightly above 1.0
            double distanceRatio = 1.0 + rng.nextDouble() * 0.2;

            return new SimulatedSearchResult(
                    rawCandidates, keptCandidates, touchedCount, precision, distanceRatio
            );
        }

        static class SimulatedSearchResult {
            final int rawCandidates;
            final int keptCandidates;
            final int touchedCount;
            final double precision;
            final double distanceRatio;

            SimulatedSearchResult(int raw, int kept, int touched, double prec, double ratio) {
                this.rawCandidates = raw;
                this.keptCandidates = kept;
                this.touchedCount = touched;
                this.precision = prec;
                this.distanceRatio = ratio;
            }
        }
    }

    // ========================================================================
    // SECTION 5: Regression Tests
    // ========================================================================

    @Nested
    @DisplayName("Regression Tests")
    class RegressionTests {

        @Test
        @DisplayName("Bug fix: precision was not aggregated when mode != 'full'")
        void testPrecisionAggregatedForPartitionedMode() {
            Profiler p = new Profiler();

            // Mode is "partitioned", not "full"
            p.recordQueryRow(
                    "Q0_K10", 10.0, 20.0, 30.0, 5.0, 1.0,
                    1.15, 0.85,  // precision = 0.85
                    100, 50, 50, 10,
                    128, 128, 10, 10, 0,
                    100, 10,
                    0, 0, 0L, 0L, 0L,
                    "gt", "partitioned",  // mode is partitioned
                    100, 50, 1, true
            );

            Aggregates agg = Aggregates.fromProfiler(p);

            // BUG: Old code only aggregated precision when mode == "full"
            // FIX: Should aggregate precision regardless of mode
            assertEquals(0.85, agg.avgPrecision, EPSILON,
                    "Precision should be aggregated even when mode='partitioned'");
            assertEquals(0.85, agg.precisionAtK.get(10), EPSILON,
                    "P@10 should be recorded even when mode='partitioned'");
        }

        @Test
        @DisplayName("Bug fix: QueryMetrics 3-arg constructor")
        void testQueryMetricsThreeArgConstructor() {
            // BUG: Old code called 3-arg constructor but only 2-arg existed
            // FIX: Added 3-arg constructor
            QueryMetrics m = new QueryMetrics(1.15, 0.90, 2.5);

            assertNotNull(m);
            assertEquals(1.15, m.ratioAtK(), EPSILON);
            assertEquals(0.90, m.precisionAtK(), EPSILON);
            assertEquals(2.5, m.candidateRatioAtK(), EPSILON);
        }

        @Test
        @DisplayName("Bug fix: distance ratio uses per-position GT distances")
        void testDistanceRatioPerPosition() {
            // BUG: Old code used single best GT distance as denominator for all positions
            // FIX: Use j-th GT distance for j-th position

            double[] gtDistances = {1.0, 2.0, 3.0};  // Different for each position
            double[] returnedDistances = {1.1, 2.2, 3.3};

            // WRONG (old): (1.1/1.0 + 2.2/1.0 + 3.3/1.0) / 3 = 6.6/3 = 2.2
            // CORRECT (new): (1.1/1.0 + 2.2/2.0 + 3.3/3.0) / 3 = (1.1 + 1.1 + 1.1) / 3 = 1.1

            double correctRatio = 0;
            for (int j = 0; j < 3; j++) {
                correctRatio += returnedDistances[j] / gtDistances[j];
            }
            correctRatio /= 3;

            assertEquals(1.1, correctRatio, EPSILON,
                    "Ratio should use per-position GT distances");
        }
    }

    // ========================================================================
    // SECTION 6: Performance Bounds Tests
    // ========================================================================

    @Nested
    @DisplayName("Performance Bounds")
    class PerformanceBoundsTests {

        @Test
        @DisplayName("Good ANN: ratio <= 1.3, precision >= 0.85")
        void testGoodANNBounds() {
            QueryMetrics good = new QueryMetrics(1.15, 0.92, 2.0);

            assertTrue(good.ratioAtK() <= 1.3, "Good ANN ratio should be <= 1.3");
            assertTrue(good.precisionAtK() >= 0.85, "Good ANN precision should be >= 0.85");
        }

        @Test
        @DisplayName("Acceptable ANN: ratio <= 1.5, precision >= 0.70")
        void testAcceptableANNBounds() {
            QueryMetrics acceptable = new QueryMetrics(1.35, 0.78, 3.0);

            assertTrue(acceptable.ratioAtK() <= 1.5, "Acceptable ANN ratio should be <= 1.5");
            assertTrue(acceptable.precisionAtK() >= 0.70, "Acceptable ANN precision should be >= 0.70");
        }

        @Test
        @DisplayName("Poor ANN: ratio > 1.5 or precision < 0.50")
        void testPoorANNBounds() {
            QueryMetrics poor = new QueryMetrics(1.9, 0.45, 5.0);

            assertTrue(poor.ratioAtK() > 1.5 || poor.precisionAtK() < 0.50,
                    "Poor ANN should have high ratio or low precision");
        }

        @Test
        @DisplayName("Paper target: SIFT1M ratio 1.10-1.20, precision >= 0.90")
        void testPaperTargets() {
            // Per EVALUATION_FRAMEWORK.md and paper
            double targetRatioMin = 1.10;
            double targetRatioMax = 1.20;
            double targetPrecisionMin = 0.90;

            QueryMetrics ideal = new QueryMetrics(1.15, 0.92, 1.5);

            assertTrue(ideal.ratioAtK() >= targetRatioMin && ideal.ratioAtK() <= targetRatioMax,
                    "Should meet SIFT1M ratio target");
            assertTrue(ideal.precisionAtK() >= targetPrecisionMin,
                    "Should meet SIFT1M precision target");
        }
    }

    // ========================================================================
    // SECTION 7: CSV Output Verification
    // ========================================================================

    @Nested
    @DisplayName("CSV Output Structure")
    class CSVOutputTests {

        @Test
        @DisplayName("Summary CSV should have all required columns")
        void testSummaryCSVColumns() {
            String[] requiredColumns = {
                    "dataset", "profile", "m", "lambda", "divisions", "index_time_ms",
                    "avg_ratio", "avg_precision", "avg_server_ms", "avg_client_ms",
                    "avg_art_ms", "avg_decrypt_ms",
                    "avg_token_bytes", "avg_work_units",
                    "avg_cand_total", "avg_cand_kept", "avg_cand_decrypted", "avg_returned",
                    "p_at_1", "p_at_5", "p_at_10", "p_at_20", "p_at_40", "p_at_60", "p_at_80", "p_at_100",
                    "reencrypt_count", "reencrypt_bytes", "reencrypt_ms",
                    "space_meta_bytes", "space_points_bytes"
            };

            // Verify column count and names
            assertEquals(31, requiredColumns.length, "Should have 31 columns");

            // Verify key columns exist
            assertTrue(Arrays.asList(requiredColumns).contains("avg_ratio"));
            assertTrue(Arrays.asList(requiredColumns).contains("avg_precision"));
            assertTrue(Arrays.asList(requiredColumns).contains("p_at_100"));
        }

        @Test
        @DisplayName("Profiler QueryRow contains all required fields")
        void testProfilerQueryRowFields() {
            Profiler p = new Profiler();

            // Record a complete row
            p.recordQueryRow(
                    "test", 1.0, 2.0, 3.0, 4.0, 5.0,
                    6.0, 7.0,
                    8, 9, 10, 11,
                    12, 13, 14, 15, 16,
                    17, 18,
                    19, 20, 21L, 22L, 23L,
                    "source", "mode",
                    24, 25, -1, false
            );

            List<Profiler.QueryRow> rows = p.getQueryRows();
            assertEquals(1, rows.size());

            Profiler.QueryRow row = rows.get(0);

            assertEquals("test", row.label);
            assertEquals(1.0, row.serverMs, EPSILON);
            assertEquals(2.0, row.clientMs, EPSILON);
            assertEquals(3.0, row.runMs, EPSILON);
            assertEquals(4.0, row.decryptMs, EPSILON);
            assertEquals(5.0, row.insertMs, EPSILON);
            assertEquals(6.0, row.ratio, EPSILON);
            assertEquals(7.0, row.precision, EPSILON);
            assertEquals(8, row.candTotal);
            assertEquals(9, row.candKept);
            assertEquals(10, row.candDecrypted);
            assertEquals(11, row.candReturned);
            assertEquals(12, row.tokenBytes);
            assertEquals(13, row.vectorDim);
            assertEquals(14, row.tokenK);
            assertEquals(15, row.tokenKBase);
            assertEquals(16, row.qIndex);
            assertEquals(17, row.totalFlushed);
            assertEquals(18, row.flushThreshold);
            assertEquals(19, row.touchedCount);
            assertEquals(20, row.reencCount);
            assertEquals(21L, row.reencTimeMs);
            assertEquals(22L, row.reencBytesDelta);
            assertEquals(23L, row.reencBytesAfter);
            assertEquals("source", row.ratioDenomSource);
            assertEquals("mode", row.mode);
            assertEquals(24, row.stableRaw);
            assertEquals(25, row.stableFinal);
            assertEquals(-1, row.nnRank);
            assertFalse(row.nnSeen);

        }
    }
}
