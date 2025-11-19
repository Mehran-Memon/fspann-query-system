package com.config;

import com.fspann.config.SystemConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class SystemConfigReencryptionFlagsTest {

    private Path writeConfig(Path dir, String name, String json) throws IOException {
        Path p = dir.resolve(name);
        Files.writeString(p, json, StandardCharsets.UTF_8);
        return p;
    }

    @Test
    void globalReencryptionEnabledWhenBothFlagsTrue(@TempDir Path tempDir) throws Exception {
        String json = """
                {
                  "reencryptionEnabled": true,
                  "reencryption": {
                    "enabled": true,
                    "batchSize": 512,
                    "maxMsPerBatch": 100
                  }
                }
                """;

        Path path = writeConfig(tempDir, "reencrypt_true.json", json);
        SystemConfig cfg = SystemConfig.load(path.toString(), true);

        assertTrue(cfg.isReencryptionEnabled());
        assertTrue(cfg.getReencryption().isEnabled());
        assertTrue(cfg.isReencryptionGloballyEnabled(),
                "Global re-encryption should be enabled when both flags are true");
    }

    @Test
    void globalReencryptionDisabledWhenTopLevelFalse(@TempDir Path tempDir) throws Exception {
        String json = """
                {
                  "reencryptionEnabled": false,
                  "reencryption": {
                    "enabled": true
                  }
                }
                """;

        Path path = writeConfig(tempDir, "reencrypt_top_disabled.json", json);
        SystemConfig cfg = SystemConfig.load(path.toString(), true);

        assertFalse(cfg.isReencryptionEnabled());
        assertTrue(cfg.getReencryption().isEnabled(),
                "Nested config can be true but global toggle overrides");
        assertFalse(cfg.isReencryptionGloballyEnabled(),
                "Global re-encryption must be disabled if top-level flag is false");
    }

    @Test
    void globalReencryptionDisabledWhenNestedDisabled(@TempDir Path tempDir) throws Exception {
        String json = """
                {
                  "reencryptionEnabled": true,
                  "reencryption": {
                    "enabled": false
                  }
                }
                """;

        Path path = writeConfig(tempDir, "reencrypt_nested_disabled.json", json);
        SystemConfig cfg = SystemConfig.load(path.toString(), true);

        assertTrue(cfg.isReencryptionEnabled());
        assertFalse(cfg.getReencryption().isEnabled());
        assertFalse(cfg.isReencryptionGloballyEnabled(),
                "Global re-encryption should be disabled if nested reencryption.enabled is false");
    }

    @Test
    void globalReencryptionEnabledWhenNestedSectionMissingUsesDefault(@TempDir Path tempDir) throws Exception {
        // no explicit "reencryption" block -> field uses default new ReencryptionConfig()
        String json = """
                {
                  "reencryptionEnabled": true
                }
                """;

        Path path = writeConfig(tempDir, "reencrypt_missing_section.json", json);
        SystemConfig cfg = SystemConfig.load(path.toString(), true);

        assertTrue(cfg.isReencryptionEnabled());
        assertNotNull(cfg.getReencryption());
        assertTrue(cfg.getReencryption().isEnabled(), "Default nested reencryption.enabled is true");
        assertTrue(cfg.isReencryptionGloballyEnabled(),
                "Global re-encryption should be enabled when both signals resolve to true");
    }
}
