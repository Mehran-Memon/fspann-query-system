package com.config;

import com.fspann.config.SystemConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class SystemConfigTest {
    @Test
    void validateThrowsOnNegativeValues(@TempDir Path tempDir) throws IOException {
        // Create a test config file with negative numShards
        Path testConfig = tempDir.resolve("test-config.json");
        String invalidConfig = """
            {
                "numShards": -1,
                "opsThreshold": 5000000,
                "ageThresholdMs": 604800000,
                "reEncBatchSize": 2000,
                "profilerEnabled": true
            }
            """;
        Files.writeString(testConfig, invalidConfig);

        // Expect ConfigLoadException due to negative numShards
        assertThrows(com.fspann.config.SystemConfig.ConfigLoadException.class, () -> {
            SystemConfig.load(testConfig.toString());
        });
    }
}