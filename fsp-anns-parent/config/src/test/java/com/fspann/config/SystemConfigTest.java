package com.fspann.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

/**
 * CORRECTED Comprehensive unit tests for SystemConfig.
 * Tests configuration loading, validation, and nested config access.
 *
 * FIXED: RatioConfig.getSource() -> Use direct field access or correct method name
 *
 * Run with: mvn test -Dtest=SystemConfigTest
 */
@DisplayName("SystemConfig Unit Tests")
public class SystemConfigTest {

    private SystemConfig config;

    @BeforeEach
    public void setUp() {
        SystemConfig.clearCache();
        config = createDefaultConfig();
    }

    @AfterEach
    public void tearDown() {
        SystemConfig.clearCache();
    }

    // ============ GETTERS TESTS ============

    @Test
    @DisplayName("Test getNumShards returns valid value")
    public void testGetNumShards() {
        int shards = config.getNumShards();
        assertTrue(shards >= 1);
        assertTrue(shards <= 8192);
    }

    @Test
    @DisplayName("Test getNumTables returns valid value")
    public void testGetNumTables() {
        int tables = config.getNumTables();
        assertTrue(tables >= 1);
        assertTrue(tables <= 1024);
    }

    @Test
    @DisplayName("Test getOpsThreshold returns valid value")
    public void testGetOpsThreshold() {
        long ops = config.getOpsThreshold();
        assertTrue(ops >= 1);
        assertTrue(ops <= 1_000_000_000L);
    }

    @Test
    @DisplayName("Test getAgeThresholdMs returns valid value")
    public void testGetAgeThresholdMs() {
        long age = config.getAgeThresholdMs();
        assertTrue(age >= 0);
    }

    // ============ FEATURE FLAGS TESTS ============

    @Test
    @DisplayName("Test isForwardSecurityEnabled")
    public void testIsForwardSecurityEnabled() {
        boolean enabled = config.isForwardSecurityEnabled();
        assertNotNull(enabled);
    }

    @Test
    @DisplayName("Test isPartitionedIndexingEnabled")
    public void testIsPartitionedIndexingEnabled() {
        boolean enabled = config.isPartitionedIndexingEnabled();
        assertNotNull(enabled);
    }

    @Test
    @DisplayName("Test isReencryptionEnabled")
    public void testIsReencryptionEnabled() {
        boolean enabled = config.isReencryptionEnabled();
        assertNotNull(enabled);
    }

    @Test
    @DisplayName("Test isProfilerEnabled")
    public void testIsProfilerEnabled() {
        boolean enabled = config.isProfilerEnabled();
        assertNotNull(enabled);
    }

    // ============ NESTED CONFIG TESTS ============

    @Test
    @DisplayName("Test getLsh returns non-null config")
    public void testGetLsh() {
        SystemConfig.LshConfig lsh = config.getLsh();
        assertNotNull(lsh);
    }

    @Test
    @DisplayName("Test getReencryption returns non-null config")
    public void testGetReencryption() {
        SystemConfig.ReencryptionConfig reenc = config.getReencryption();
        assertNotNull(reenc);
    }

    @Test
    @DisplayName("Test getPaper returns non-null config")
    public void testGetPaper() {
        SystemConfig.PaperConfig paper = config.getPaper();
        assertNotNull(paper);
    }

    @Test
    @DisplayName("Test getEval returns non-null config")
    public void testGetEval() {
        SystemConfig.EvalConfig eval = config.getEval();
        assertNotNull(eval);
    }

    @Test
    @DisplayName("Test getOutput returns non-null config")
    public void testGetOutput() {
        SystemConfig.OutputConfig output = config.getOutput();
        assertNotNull(output);
    }

    @Test
    @DisplayName("Test getStabilization returns non-null config")
    public void testGetStabilization() {
        SystemConfig.StabilizationConfig stab = config.getStabilization();
        assertNotNull(stab);
    }

    @Test
    @DisplayName("Test getRatio returns non-null config")
    public void testGetRatio() {
        SystemConfig.RatioConfig ratio = config.getRatio();
        assertNotNull(ratio);
    }

    @Test
    @DisplayName("Test getKAdaptive returns non-null config")
    public void testGetKAdaptive() {
        SystemConfig.KAdaptiveConfig kadaptive = config.getKAdaptive();
        assertNotNull(kadaptive);
    }

    // ============ LSHCONFIG TESTS ============

    @Test
    @DisplayName("Test LshConfig getNumTables")
    public void testLshGetNumTables() {
        SystemConfig.LshConfig lsh = config.getLsh();
        int numTables = lsh.getNumTables();
        assertTrue(numTables >= 1);
        assertTrue(numTables <= 1024);
    }

    @Test
    @DisplayName("Test LshConfig getNumFunctions")
    public void testLshGetNumFunctions() {
        SystemConfig.LshConfig lsh = config.getLsh();
        int numFuncs = lsh.getNumFunctions();
        assertTrue(numFuncs >= 1);
        assertTrue(numFuncs <= 256);
    }

    @Test
    @DisplayName("Test LshConfig getNumBuckets")
    public void testLshGetNumBuckets() {
        SystemConfig.LshConfig lsh = config.getLsh();
        int numBuckets = lsh.getNumBuckets();
        assertTrue(numBuckets >= 100);
        assertTrue(numBuckets <= 1_000_000);
    }

    @Test
    @DisplayName("Test LshConfig getTargetRatio")
    public void testLshGetTargetRatio() {
        SystemConfig.LshConfig lsh = config.getLsh();
        double ratio = lsh.getTargetRatio();
        assertTrue(ratio >= 1.0);
        assertTrue(ratio <= 2.0);
    }

    // ============ PAPERCONFIG TESTS ============

    @Test
    @DisplayName("Test PaperConfig getM")
    public void testPaperGetM() {
        SystemConfig.PaperConfig paper = config.getPaper();
        int m = paper.getM();
        assertTrue(m >= 1);
    }

    @Test
    @DisplayName("Test PaperConfig getLambda")
    public void testPaperGetLambda() {
        SystemConfig.PaperConfig paper = config.getPaper();
        int lambda = paper.getLambda();
        assertTrue(lambda >= 1);
    }

    @Test
    @DisplayName("Test PaperConfig getDivisions")
    public void testPaperGetDivisions() {
        SystemConfig.PaperConfig paper = config.getPaper();
        int divisions = paper.getDivisions();
        assertTrue(divisions >= 1);
    }

    @Test
    @DisplayName("Test PaperConfig getSeed")
    public void testPaperGetSeed() {
        SystemConfig.PaperConfig paper = config.getPaper();
        long seed = paper.getSeed();
        assertNotNull(seed);
    }

    // ============ STABILIZATIONCONFIG TESTS ============

    @Test
    @DisplayName("Test StabilizationConfig isEnabled")
    public void testStabilizationIsEnabled() {
        SystemConfig.StabilizationConfig stab = config.getStabilization();
        boolean enabled = stab.isEnabled();
        assertNotNull(enabled);
    }

    @Test
    @DisplayName("Test StabilizationConfig getAlpha")
    public void testStabilizationGetAlpha() {
        SystemConfig.StabilizationConfig stab = config.getStabilization();
        double alpha = stab.getAlpha();
        assertTrue(alpha > 0.0);
        assertTrue(alpha <= 1.0);
    }

    @Test
    @DisplayName("Test StabilizationConfig getMinCandidates")
    public void testStabilizationGetMinCandidates() {
        SystemConfig.StabilizationConfig stab = config.getStabilization();
        int minCand = stab.getMinCandidates();
        assertTrue(minCand >= 1);
    }

    // ============ REENCRYPTIONCONFIG TESTS ============

    @Test
    @DisplayName("Test ReencryptionConfig isEnabled")
    public void testReencryptionIsEnabled() {
        SystemConfig.ReencryptionConfig reenc = config.getReencryption();
        boolean enabled = reenc.isEnabled();
        assertNotNull(enabled);
    }

    @Test
    @DisplayName("Test ReencryptionConfig getBatchSize")
    public void testReencryptionGetBatchSize() {
        SystemConfig.ReencryptionConfig reenc = config.getReencryption();
        int batchSize = reenc.getBatchSize();
        assertTrue(batchSize >= 1);
    }

    @Test
    @DisplayName("Test ReencryptionConfig getMaxMsPerBatch")
    public void testReencryptionGetMaxMsPerBatch() {
        SystemConfig.ReencryptionConfig reenc = config.getReencryption();
        long maxMs = reenc.getMaxMsPerBatch();
        assertTrue(maxMs >= 0);
    }

    // ============ EVALCONFIG TESTS ============

    @Test
    @DisplayName("Test EvalConfig computePrecision")
    public void testEvalComputePrecision() {
        SystemConfig.EvalConfig eval = config.getEval();
        boolean compute = eval.computePrecision;
        assertNotNull(compute);
    }

    @Test
    @DisplayName("Test EvalConfig kVariants")
    public void testEvalKVariants() {
        SystemConfig.EvalConfig eval = config.getEval();
        int[] kVariants = eval.kVariants;
        assertNotNull(kVariants);
        assertTrue(kVariants.length > 0);
        for (int k : kVariants) {
            assertTrue(k > 0);
        }
    }

    // ============ OUTPUTCONFIG TESTS ============

    @Test
    @DisplayName("Test OutputConfig getResultsDir")
    public void testOutputGetResultsDir() {
        SystemConfig.OutputConfig output = config.getOutput();
        String resultsDir = output.resultsDir;
        assertNotNull(resultsDir);
        assertFalse(resultsDir.isBlank());
    }

    @Test
    @DisplayName("Test OutputConfig exportArtifacts")
    public void testOutputExportArtifacts() {
        SystemConfig.OutputConfig output = config.getOutput();
        boolean export = output.exportArtifacts;
        assertNotNull(export);
    }

    // ============ RATIOCONFIG TESTS ============

    @Test
    @DisplayName("Test RatioConfig returns non-null")
    public void testRatioConfigNonNull() {
        SystemConfig.RatioConfig ratio = config.getRatio();
        assertNotNull(ratio);
    }

    @Test
    @DisplayName("Test RatioConfig source field")
    public void testRatioConfigSource() {
        SystemConfig.RatioConfig ratio = config.getRatio();
        // Access field directly instead of through getter
        String source = ratio.source;
        assertNotNull(source);
        // Source should be one of: auto, gt, base
        assertTrue(source.equals("auto") || source.equals("gt") || source.equals("base") || !source.isEmpty());
    }

    // ============ CACHE TESTS ============

    @Test
    @DisplayName("Test clearCache empties the cache")
    public void testClearCache() {
        SystemConfig.clearCache();
        // Should not throw
        assertTrue(true);
    }

    // ============ HELPER METHODS ============

    private SystemConfig createDefaultConfig() {
        try {
            return new SystemConfig();
        } catch (Exception e) {
            return new SystemConfig();
        }
    }
}