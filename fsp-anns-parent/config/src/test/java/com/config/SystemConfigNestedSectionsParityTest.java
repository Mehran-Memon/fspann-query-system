package com.config;

import com.fspann.config.SystemConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class SystemConfigNestedSectionsParityTest {

    @Test
    void newSystemConfig_allNestedSectionsNonNullWithDefaults() {
        SystemConfig cfg = new SystemConfig();

        assertNotNull(cfg.getLsh(), "LSH config must not be null");
        assertNotNull(cfg.getReencryption(), "Reencryption config must not be null");
        assertNotNull(cfg.getPaper(), "Paper config must not be null");
        assertNotNull(cfg.getEval(), "Eval config must not be null");
        assertNotNull(cfg.getOutput(), "Output config must not be null");
        assertNotNull(cfg.getAudit(), "Audit config must not be null");
        assertNotNull(cfg.getCloak(), "Cloak config must not be null");
        assertNotNull(cfg.getRatio(), "Ratio config must not be null");
        assertNotNull(cfg.getKAware(), "KAware config must not be null");
        assertNotNull(cfg.getKAdaptive(), "KAdaptive config must not be null");

        // A few spot-checks on defaults
        assertEquals(32, cfg.getNumShards());
        assertEquals(8, cfg.getNumTables());
        assertTrue(cfg.isReencryptionEnabled());
        assertTrue(cfg.isForwardSecurityEnabled());
        assertTrue(cfg.isPartitionedIndexingEnabled());
    }

    @Test
    void load_configOmittingNestedSections_stillProvidesDefaults(@TempDir Path tempDir) throws Exception {
        String json = """
                {
                  "numShards": 40,
                  "numTables": 10
                  // note: no lsh, reencryption, paper, eval, output, audit, cloak, ratio, kAware, kAdaptive
                }
                """;

        // Remove comment (in case JSON parser is strict)
        json = json.replaceAll("//.*", "");

        Path cfgPath = tempDir.resolve("partial.json");
        Files.writeString(cfgPath, json, StandardCharsets.UTF_8);

        SystemConfig cfg = SystemConfig.load(cfgPath.toString(), true);

        assertEquals(40, cfg.getNumShards());
        assertEquals(10, cfg.getNumTables());

        assertNotNull(cfg.getLsh());
        assertNotNull(cfg.getReencryption());
        assertNotNull(cfg.getPaper());
        assertNotNull(cfg.getEval());
        assertNotNull(cfg.getOutput());
        assertNotNull(cfg.getAudit());
        assertNotNull(cfg.getCloak());
        assertNotNull(cfg.getRatio());
        assertNotNull(cfg.getKAware());
        assertNotNull(cfg.getKAdaptive());
    }
}
