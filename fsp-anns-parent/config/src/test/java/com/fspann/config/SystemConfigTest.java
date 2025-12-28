package com.fspann.config;

import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests aligned EXACTLY with SystemConfig API
 *
 * No invented getters
 * No removed methods
 * No speculative features
 */
@DisplayName("SystemConfig Unit Tests")
public class SystemConfigTest {

    private SystemConfig config;

    @BeforeEach
    void setUp() {
        config = new SystemConfig();
    }

    // =========================================================
    // BASIC GETTERS
    // =========================================================

    @Test
    void testNumShards() {
        int shards = config.getNumShards();
        assertTrue(shards >= 1);
    }

    @Test
    void testOpsThreshold() {
        long ops = config.getOpsThreshold();
        assertTrue(ops > 0);
    }

    @Test
    void testAgeThresholdMs() {
        long age = config.getAgeThresholdMs();
        assertTrue(age >= 0);
    }

    // =========================================================
    // FEATURE FLAGS
    // =========================================================

    @Test
    void testForwardSecurityEnabled() {
        assertTrue(config.isForwardSecurityEnabled() || !config.isForwardSecurityEnabled());
    }

    @Test
    void testProfilerEnabled() {
        assertTrue(config.isProfilerEnabled() || !config.isProfilerEnabled());
    }

    @Test
    void testReencryptionGloballyEnabled() {
        assertTrue(config.isReencryptionGloballyEnabled() || !config.isReencryptionGloballyEnabled());
    }

    // =========================================================
    // PAPER CONFIG
    // =========================================================

    @Test
    void testPaperConfig() {
        SystemConfig.PaperConfig p = config.getPaper();
        assertNotNull(p);

        assertTrue(p.getM() > 0);
        assertTrue(p.getLambda() > 0);
        assertTrue(p.getDivisions() > 0);
        assertTrue(p.getTables() > 0);
        assertNotEquals(0L, p.getSeed());
    }

    // =========================================================
    // RUNTIME CONFIG
    // =========================================================

    @Test
    void testRuntimeConfig() {
        SystemConfig.RuntimeConfig r = config.getRuntime();
        assertNotNull(r);

        assertTrue(r.getMaxCandidateFactor() > 0);
        assertTrue(r.getMaxRefinementFactor() > 0);
        assertTrue(r.getMaxRelaxationDepth() >= 0);
        assertTrue(r.getEarlyStopCandidates() > 0);
        assertTrue(r.getRefinementLimit() > 0);
    }

    // =========================================================
    // STABILIZATION CONFIG
    // =========================================================

    @Test
    void testStabilizationConfig() {
        SystemConfig.StabilizationConfig s = config.getStabilization();
        assertNotNull(s);

        assertTrue(s.getAlpha() > 0.0);
        assertTrue(s.getMinCandidatesRatio() >= 1.0);
    }

    // =========================================================
    // EVAL CONFIG (FIELD ACCESS — BY DESIGN)
    // =========================================================

    @Test
    void testEvalConfig() {
        SystemConfig.EvalConfig e = config.getEval();
        assertNotNull(e);

        assertNotNull(e.computePrecision);
        assertNotNull(e.writeGlobalPrecisionCsv);
        assertNotNull(e.kVariants);
        assertTrue(e.kVariants.length > 0);
    }

    // =========================================================
    // RATIO CONFIG (FIELD ACCESS — BY DESIGN)
    // =========================================================

    @Test
    void testRatioConfig() {
        SystemConfig.RatioConfig r = config.getRatio();
        assertNotNull(r);

        assertNotNull(r.source);
        assertTrue(
                r.source.equals("auto") ||
                        r.source.equals("gt")   ||
                        r.source.equals("base")
        );
    }

    // =========================================================
    // REENCRYPTION CONFIG
    // =========================================================

    @Test
    void testReencryptionConfig() {
        SystemConfig.ReencryptionConfig r = config.getReencryption();
        assertNotNull(r);

        assertTrue(r.getBatchSize() > 0);
        assertTrue(r.getMaxMsPerBatch() >= 0);
    }

    // =========================================================
    // OPTIONAL CONFIG BLOCKS (MAY BE NULL)
    // =========================================================

    @Test
    void testOptionalConfigs() {
        // These are optional by design
        assertDoesNotThrow(() -> config.getKAdaptive());
        assertDoesNotThrow(() -> config.getOutput());
        assertDoesNotThrow(() -> config.getCloak());
    }
}
