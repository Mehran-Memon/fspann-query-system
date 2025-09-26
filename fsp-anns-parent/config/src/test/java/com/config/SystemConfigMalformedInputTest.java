package com.config;

import com.fspann.config.SystemConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class SystemConfigMalformedInputTest {

    @Test
    void malformedYamlThrows(@TempDir Path dir) throws Exception {
        Path cfg = dir.resolve("conf.yaml");
        Files.writeString(cfg, """
            numShards: 8
            eval:
              computePrecision: true
            ratio:
              source: gt
              gtSample: not-a-number   # <-- malformed
            """);

        SystemConfig.clearCache();
        assertThrows(SystemConfig.ConfigLoadException.class, () -> SystemConfig.load(cfg.toString(), true));
    }

    @Test
    void unknownFieldsAreIgnored(@TempDir Path dir) throws Exception {
        Path cfg = dir.resolve("conf.json");
        Files.writeString(cfg, """
            {
              "numShards": 8,
              "eval": { "computePrecision": true },
              "someNewThing": { "foo": 1, "bar": 2 }
            }
            """);

        SystemConfig.clearCache();
        SystemConfig c = SystemConfig.load(cfg.toString(), true);

        assertEquals(8, c.getNumShards());
        assertTrue(c.getEval().computePrecision);
        // Test passes if load succeeds and known fields are preserved.
    }

    @Test
    void numericClampsAlsoAcceptStrings(@TempDir Path dir) throws Exception {
        // Some deploy pipelines emit numbers as strings; verify we still clamp.
        Path cfg = dir.resolve("conf.yaml");
        Files.writeString(cfg, """
            numShards: "100000"
            opsThreshold: "5000000000"
            ageThresholdMs: "999999999999"
            reEncBatchSize: "500000"
            """);

        SystemConfig.clearCache();
        SystemConfig c = SystemConfig.load(cfg.toString(), true);

        assertEquals(8192, c.getNumShards());
        assertEquals(1_000_000_000L, c.getOpsThreshold());
        assertEquals(30L*24*60*60*1000, c.getAgeThresholdMs());
        assertEquals(10_000, c.getReEncBatchSize());
    }
}
