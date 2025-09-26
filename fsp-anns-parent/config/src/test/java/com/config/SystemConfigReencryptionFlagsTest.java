package com.config;

import com.fspann.config.SystemConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class SystemConfigReencryptionFlagsTest {

    @Test
    void legacyAndNestedEnabledMustBothBeTrue(@TempDir Path dir) throws Exception {
        Path cfg = dir.resolve("conf.yaml");
        Files.writeString(cfg, """
            # legacy top-level switch
            reencryptionGloballyEnabled: true
            reencryption:
              enabled: true
            """);

        SystemConfig.clearCache();
        SystemConfig c = SystemConfig.load(cfg.toString(), true);

        assertTrue(c.isReencryptionGloballyEnabled(), "top-level should be true");
        assertNotNull(c.getReencryption(), "nested struct should exist");
        assertTrue(c.getReencryption().enabled, "nested should be true");
    }

    @Test
    void nestedFalseDisablesEvenIfLegacyTrue(@TempDir Path dir) throws Exception {
        Path cfg = dir.resolve("conf.yaml");
        Files.writeString(cfg, """
            reencryptionGloballyEnabled: true
            reencryption:
              enabled: false
            """);

        SystemConfig.clearCache();
        SystemConfig c = SystemConfig.load(cfg.toString(), true);

        assertTrue(c.isReencryptionGloballyEnabled());
        assertFalse(c.getReencryption().enabled, "nested false should be honored");
        // Call sites AND these flags (legacy && nested); this ensures the effective gate will be false.
    }

    @Test
    void legacyFalseWinsEvenIfNestedTrue(@TempDir Path dir) throws Exception {
        Path cfg = dir.resolve("conf.yaml");
        Files.writeString(cfg, """
            reencryptionGloballyEnabled: false
            reencryption:
              enabled: true
            """);

        SystemConfig.clearCache();
        SystemConfig c = SystemConfig.load(cfg.toString(), true);

        assertFalse(c.isReencryptionGloballyEnabled(), "top-level false");
        assertTrue(c.getReencryption().enabled, "nested true loaded");
        // Effective gate at call sites: legacy(false) && nested(true) -> false
    }
}
