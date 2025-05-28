package com.fspann.api;

import com.fspann.config.SystemConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class ApiSystemConfigTest {

    @TempDir Path tempDir;

    @Test
    void loadValidConfigFile() throws Exception {
        // create a minimal JSON file (content can be empty object if load uses defaults)
        Path configFile = tempDir.resolve("config.json");
        Files.writeString(configFile, "{}");

                ApiSystemConfig apiConfig = new ApiSystemConfig(configFile.toString());
        SystemConfig cfg = apiConfig.getConfig();
        assertNotNull(cfg, "SystemConfig should not be null");
        // default values should be accessible via getters
        assertEquals(32, cfg.getNumShards(), "Default numShards should be 32");
        assertEquals(5_000_000, cfg.getOpsThreshold(), "Default opsThreshold should be 5_000_000");
        assertEquals(7L * 24 * 60 * 60 * 1000, cfg.getAgeThresholdMs(), "Default ageThresholdMs should match 7 days");
    }

    @Test
    void loadMissingConfigFileThrows() {
        String missingPath = tempDir.resolve("missing.json").toString();
        assertThrows(Exception.class, () -> new ApiSystemConfig(missingPath));
    }
}