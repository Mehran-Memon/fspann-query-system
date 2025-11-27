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
        // Create a minimal JSON file with realistic configuration
        Path configFile = tempDir.resolve("config_fixedK.json");
        String configJson = """
            {
                "numShards": 4,
                "opsThreshold": 1000000,
                "ageThresholdMs": 604800000,
                "profilerEnabled": true
            }
            """;
        Files.writeString(configFile, configJson);

        ApiSystemConfig apiConfig = new ApiSystemConfig(configFile.toString());
        SystemConfig cfg = apiConfig.getConfig();
        assertNotNull(cfg, "SystemConfig should not be null");
        assertEquals(4, cfg.getNumShards(), "numShards should be 4");
        assertEquals(1_000_000, cfg.getOpsThreshold(), "opsThreshold should be 1,000,000");
        assertEquals(7L * 24 * 60 * 60 * 1000, cfg.getAgeThresholdMs(), "ageThresholdMs should match 7 days");
        assertTrue(cfg.isProfilerEnabled(), "profilerEnabled should be true");
    }

    @Test
    void loadMissingConfigFileThrows() {
        String missingPath = tempDir.resolve("missing.json").toString();
        assertThrows(Exception.class, () -> new ApiSystemConfig(missingPath), "Should throw exception for missing config file");
    }

    @Test
    void loadInvalidConfigFileThrows() throws IOException {
        // Create an invalid JSON file
        Path configFile = tempDir.resolve("invalid.json");
        Files.writeString(configFile, "{ invalid json }");
        assertThrows(Exception.class, () -> new ApiSystemConfig(configFile.toString()), "Should throw exception for invalid JSON");
    }
}