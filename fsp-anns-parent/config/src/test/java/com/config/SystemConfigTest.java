package com.config;

import com.fspann.config.SystemConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class SystemConfigTest {

    private static final long MAX_AGE_MS = 31_536_000_000L;      // 365 days in ms
    private static final long MAX_OPS    = 1_000_000_000L;
    private static final int  MAX_SHARDS = 8_192;
    private static final int  MAX_TABLES = 1_024;

    private Path writeConfig(Path dir, String fileName, String json) throws IOException {
        Path p = dir.resolve(fileName);
        Files.writeString(p, json, StandardCharsets.UTF_8);
        return p;
    }

    @Test
    void load_validConfig_appliesClampingAndDefaults(@TempDir Path tempDir) throws Exception {
        String json = """
                {
                  "numShards": 10000,
                  "numTables": 2000,
                  "opsThreshold": 2000000000,
                  "ageThresholdMs": 999999999999,
                  "reencryptionEnabled": true,
                  "forwardSecurityEnabled": true,
                  "partitionedIndexingEnabled": true,
                  "profilerEnabled": true,
                  "lsh": {
                    "numTables": 2048,
                    "rowsPerBand": 4,
                    "probeShards": 9000
                  },
                  "kAdaptive": {
                    "enabled": true,
                    "maxRounds": 5,
                    "targetReturnRate": 0.9,
                    "maxFanout": 8000.0,
                    "probeFactor": 2.0
                  }
                }
                """;

        Path cfgPath = writeConfig(tempDir, "config.json", json);

        SystemConfig cfg = SystemConfig.load(cfgPath.toString(), true);

        // top-level clamping
        assertEquals(MAX_SHARDS, cfg.getNumShards(), "numShards should be clamped to MAX_SHARDS");
        assertEquals(MAX_TABLES, cfg.getNumTables(), "numTables should be clamped to MAX_TABLES");
        assertEquals(MAX_OPS, cfg.getOpsThreshold(), "opsThreshold should be clamped to MAX_OPS");
        assertEquals(MAX_AGE_MS, cfg.getAgeThresholdMs(), "ageThresholdMs should be clamped to MAX_AGE_MS");

        assertTrue(cfg.isForwardSecurityEnabled());
        assertTrue(cfg.isPartitionedIndexingEnabled());
        assertTrue(cfg.isReencryptionEnabled());
        assertTrue(cfg.isProfilerEnabled());

        // LSH nested config
        assertNotNull(cfg.getLsh());
        assertEquals(MAX_TABLES, cfg.getLsh().getNumTables(), "LSH numTables should be clamped");
        assertEquals(MAX_SHARDS, cfg.getLsh().getProbeShards(), "LSH probeShards should be clamped");

        // kAdaptive nested config
        SystemConfig.KAdaptiveConfig ka = cfg.getKAdaptive();
        assertNotNull(ka);
        assertTrue(ka.enabled, "kAdaptive.enabled should be true");
        assertEquals(5, ka.maxRounds);
        assertEquals(0.9, ka.targetReturnRate, 1e-9);
        assertEquals(8000.0, ka.maxFanout, 1e-9);
        assertEquals(2.0, ka.probeFactor, 1e-9);
    }

    @Test
    void load_usesCacheWhenRefreshFalseAndSamePath(@TempDir Path tempDir) throws Exception {
        String json1 = """
                {
                  "numShards": 64
                }
                """;
        Path cfgPath = writeConfig(tempDir, "config.json", json1);

        SystemConfig.clearCache();
        SystemConfig first = SystemConfig.load(cfgPath.toString(), false);
        assertEquals(64, first.getNumShards());

        // overwrite file with different value
        String json2 = """
                {
                  "numShards": 16
                }
                """;
        writeConfig(tempDir, "config.json", json2);

        // without refresh, should still get cached instance
        SystemConfig second = SystemConfig.load(cfgPath.toString(), false);
        assertSame(first, second, "Expected cached instance when refresh=false");
        assertEquals(64, second.getNumShards(), "Cached value should still be 64");

        // with refresh, should reload and reflect new value
        SystemConfig refreshed = SystemConfig.load(cfgPath.toString(), true);
        assertNotSame(first, refreshed, "Refresh=true should bypass cache");
        assertEquals(16, refreshed.getNumShards());
    }

    @Test
    void load_unknownFieldsAreIgnored(@TempDir Path tempDir) throws Exception {
        String json = """
                {
                  "numShards": 48,
                  "someUnknownField": "ignored",
                  "lsh": {
                    "numTables": 16,
                    "unknownLshField": 123
                  },
                  "kAdaptive": {
                    "enabled": true,
                    "maxRounds": 2,
                    "targetReturnRate": 0.85,
                    "maxFanout": 3000.0,
                    "probeFactor": 1.25,
                    "anotherUnknownField": "x"
                  }
                }
                """;

        Path cfgPath = writeConfig(tempDir, "cfg_unknown.json", json);
        SystemConfig cfg = SystemConfig.load(cfgPath.toString(), true);

        assertEquals(48, cfg.getNumShards());
        assertEquals(16, cfg.getLsh().getNumTables());
        assertTrue(cfg.getKAdaptive().enabled);
        assertEquals(2, cfg.getKAdaptive().maxRounds);
        assertEquals(0.85, cfg.getKAdaptive().targetReturnRate, 1e-9);
    }
}
