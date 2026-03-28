package com.fspann.api;

import com.fspann.config.SystemConfig;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ApiSystemConfig Tests")
class ApiSystemConfigTest {

    private Path tempConfig;

    @BeforeEach
    void setUp() throws IOException {
        tempConfig = Files.createTempFile("fsp-config", ".json");

        // Minimal valid config JSON
        Files.writeString(
                tempConfig,
                """
                {
                  "numShards": 16,
                  "opsThreshold": 1000000,
                  "ageThresholdMs": 1000,
                  "paper": {
                    "m": 24,
                    "lambda": 2,
                    "divisions": 3,
                    "tables": 6,
                    "seed": 13
                  }
                }
                """
        );

        // Ensure clean API cache only
        ApiSystemConfig.clearCache();
        System.clearProperty("config.refresh");
    }

    @AfterEach
    void tearDown() throws IOException {
        Files.deleteIfExists(tempConfig);
        ApiSystemConfig.clearCache();
    }

    @Test
    @DisplayName("Loads configuration successfully")
    void testLoadConfig() throws Exception {
        ApiSystemConfig apiCfg = new ApiSystemConfig(tempConfig.toString());
        SystemConfig cfg = apiCfg.getConfig();

        assertNotNull(cfg);
        assertEquals(16, cfg.getNumShards());
        assertEquals(24, cfg.getPaper().getM());
        assertEquals(3, cfg.getPaper().getDivisions());
    }

    @Test
    @DisplayName("Uses cached configuration by default")
    void testConfigCaching() throws Exception {
        ApiSystemConfig first = new ApiSystemConfig(tempConfig.toString());
        ApiSystemConfig second = new ApiSystemConfig(tempConfig.toString());

        assertSame(
                first.getConfig(),
                second.getConfig(),
                "Expected cached SystemConfig instance"
        );
    }

    @Test
    @DisplayName("Reloads configuration when config.refresh=true")
    void testConfigRefresh() throws Exception {
        ApiSystemConfig first = new ApiSystemConfig(tempConfig.toString());

        System.setProperty("config.refresh", "true");

        ApiSystemConfig second = new ApiSystemConfig(tempConfig.toString());

        assertNotSame(
                first.getConfig(),
                second.getConfig(),
                "Expected new SystemConfig instance after refresh"
        );
    }

    @Test
    @DisplayName("Throws IOException for unreadable config")
    void testUnreadableConfigThrows() {
        Path badPath = Path.of("does-not-exist.json");

        assertThrows(
                IOException.class,
                () -> new ApiSystemConfig(badPath.toString())
        );
    }
}

