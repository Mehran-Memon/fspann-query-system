package com.config;

import com.fspann.config.SystemConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class SystemConfigPaperValidationTest {

    private Path writeConfig(Path dir, String name, String json) throws IOException {
        Path p = dir.resolve(name);
        Files.writeString(p, json, StandardCharsets.UTF_8);
        return p;
    }

    @Test
    void paperConfig_clampsValuesAndRespectsEnabledFlag(@TempDir Path tempDir) throws Exception {
        String json = """
                {
                  "paper": {
                    "enabled": true,
                    "m": 0,
                    "lambda": -5,
                    "divisions": 0,
                    "seed": 99,
                    "safetyMaxCandidates": -1
                  }
                }
                """;

        Path path = writeConfig(tempDir, "paper.json", json);
        SystemConfig cfg = SystemConfig.load(path.toString(), true);
        SystemConfig.PaperConfig pc = cfg.getPaper();

        assertTrue(pc.isEnabled(), "PaperConfig.enabled must be true");
        assertEquals(1, pc.getM(), "m must be clamped to at least 1");
        assertEquals(1, pc.getLambda(), "lambda must be clamped to at least 1");
        assertEquals(1, pc.getDivisions(), "divisions must be clamped to at least 1");
        assertEquals(99L, pc.getSeed());
        assertEquals(0, pc.getSafetyMaxCandidates(), "safetyMaxCandidates must be >= 0");
    }

    @Test
    void paperConfig_defaultsAreSaneWhenSectionMissing(@TempDir Path tempDir) throws Exception {
        String json = """
                {
                  "numShards": 32
                }
                """;

        Path path = writeConfig(tempDir, "nopaper.json", json);
        SystemConfig cfg = SystemConfig.load(path.toString(), true);

        SystemConfig.PaperConfig pc = cfg.getPaper();
        assertNotNull(pc);
        assertFalse(pc.isEnabled(), "Default paper.enabled should be false");
        assertEquals(12, pc.getM());
        assertEquals(6, pc.getLambda());
        assertEquals(8, pc.getDivisions());
        assertEquals(42L, pc.getSeed());
    }
}
