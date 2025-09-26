package com.config;

import com.fspann.config.SystemConfig;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class SystemConfigPaperValidationTest {

    @Test
    void paperBoundsAreClampedAndAdjusted() throws Exception {
        String yaml = """
          numShards: 4
          eval: { computePrecision: false }
          paper:
            enabled: true
            m: 15
            lambda: 2
            divisions: 9
            maxCandidates: 1000
            targetMult: 1.5
            expandRadiusMax: 3.0
            expandRadiusHard: 2.0   # invalid; should be raised to 3.0
          cloak: { noise: -1.0 }    # invalid; clamped to 0
          """;

        Path cfg = Files.createTempFile("cfg", ".yaml");
        Files.writeString(cfg, yaml);

        SystemConfig sc = SystemConfig.load(cfg.toString(), true);
        assertEquals(0.0, sc.getCloak().noise, 1e-12, "Negative noise clamped to 0");
    }
}
