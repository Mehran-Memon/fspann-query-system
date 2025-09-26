package com.config;

import com.fspann.config.SystemConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class SystemConfigNestedSectionsParityTest {

    @Test
    void yamlAndJsonYieldSameValues(@TempDir Path dir) throws Exception {
        String yaml = """
            numShards: 16
            eval:
              computePrecision: true
              writeGlobalPrecisionCsv: false
            audit:
              enable: true
              k: 50
              sampleEvery: 10
              worstKeep: 7
            ratio:
              source: "gt"
              gtSample: 25
              gtMismatchTolerance: 0.15
            cloak:
              noise: 0.02
            lsh:
              numTables: 6
              rowsPerBand: 12
              probeShards: 4
            """;

        String json = """
            {
              "numShards": 16,
              "eval": { "computePrecision": true, "writeGlobalPrecisionCsv": false },
              "audit": { "enable": true, "k": 50, "sampleEvery": 10, "worstKeep": 7 },
              "ratio": { "source": "gt", "gtSample": 25, "gtMismatchTolerance": 0.15 },
              "cloak": { "noise": 0.02 },
              "lsh": { "numTables": 6, "rowsPerBand": 12, "probeShards": 4 }
            }
            """;

        Path ymlPath = dir.resolve("conf.yaml");
        Path jsonPath = dir.resolve("conf.json");
        Files.writeString(ymlPath, yaml);
        Files.writeString(jsonPath, json);

        SystemConfig.clearCache();
        SystemConfig cy = SystemConfig.load(ymlPath.toString(), true);

        SystemConfig.clearCache();
        SystemConfig cj = SystemConfig.load(jsonPath.toString(), true);

        assertEquals(cy.getNumShards(), cj.getNumShards());

        // eval
        assertEquals(cy.getEval().computePrecision, cj.getEval().computePrecision);
        assertEquals(cy.getEval().writeGlobalPrecisionCsv, cj.getEval().writeGlobalPrecisionCsv);

        // audit
        assertEquals(cy.getAudit().enable, cj.getAudit().enable);
        assertEquals(cy.getAudit().k, cj.getAudit().k);
        assertEquals(cy.getAudit().sampleEvery, cj.getAudit().sampleEvery);
        assertEquals(cy.getAudit().worstKeep, cj.getAudit().worstKeep);

        // ratio
        assertEquals(cy.getRatio().source, cj.getRatio().source);
        assertEquals(cy.getRatio().gtSample, cj.getRatio().gtSample);
        assertEquals(cy.getRatio().gtMismatchTolerance, cj.getRatio().gtMismatchTolerance, 1e-12);

        // cloak
        assertEquals(cy.getCloak().noise, cj.getCloak().noise, 1e-12);

        // lsh
        assertEquals(cy.getLsh().numTables, cj.getLsh().numTables);
        assertEquals(cy.getLsh().rowsPerBand, cj.getLsh().rowsPerBand);
        assertEquals(cy.getLsh().probeShards, cj.getLsh().probeShards);
    }
}
