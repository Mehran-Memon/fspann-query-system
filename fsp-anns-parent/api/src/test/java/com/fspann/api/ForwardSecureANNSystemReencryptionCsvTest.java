package com.fspann.api;

import com.fspann.common.RocksDBMetadataManager;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.crypto.CryptoService;
import com.fspann.key.KeyManager;
import com.fspann.key.KeyRotationPolicy;
import com.fspann.key.KeyRotationServiceImpl;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ForwardSecureANNSystemReencryptionCsvTest {

    @TempDir
    Path tempDir;

    @BeforeAll
    static void enableTestMode() {
        System.setProperty("test.env", "true");
    }

    @AfterEach
    void clearProps() {
        System.clearProperty("results.dir");
        System.clearProperty("reenc.mode");
    }

    @Test
    void shutdown_writesSummaryLineToReencryptMetricsCsv() throws Exception {
        Path resultsDir = tempDir.resolve("results");
        Files.createDirectories(resultsDir);

        ForwardSecureANNSystem system = null;
        RocksDBMetadataManager metadataManager = null;

        try {
            // Build config with explicit reencryption block
            Path configFile = tempDir.resolve("config.json");
            String escapedResults = resultsDir.toString().replace("\\", "\\\\");
            String configJson = """
            {
              "numShards": 4,
              "profilerEnabled": false,
              "opsThreshold": 2147483647,
              "ageThresholdMs": 9223372036854775807,
              "lsh": { "numTables": 2, "rowsPerBand": 2, "probeShards": 1 },
              "eval": {
                "computePrecision": true,
                "writeGlobalPrecisionCsv": false,
                "kVariants": [1]
              },
              "cloak": { "noise": 0.0 },
              "ratio": {
                "source": "base",
                "gtSample": 0,
                "gtMismatchTolerance": 1.0
              },
              "reencryption": {
                "enabled": true
              },
              "output": {
                "resultsDir": "%s",
                "exportArtifacts": false
              }
            }
            """.formatted(escapedResults);
            Files.writeString(configFile, configJson);

            Path keysDir = tempDir.resolve("keys");
            Path metadataDir = tempDir.resolve("metadata");
            Path pointsDir = tempDir.resolve("points");
            Files.createDirectories(keysDir);
            Files.createDirectories(metadataDir);
            Files.createDirectories(pointsDir);

            metadataManager = RocksDBMetadataManager.create(metadataDir.toString(), pointsDir.toString());
            KeyManager keyManager = new KeyManager(keysDir.toString());
            KeyRotationPolicy policy = new KeyRotationPolicy(2_000_000, 1_000_000);
            KeyRotationServiceImpl keyService =
                    new KeyRotationServiceImpl(keyManager, policy, metadataDir.toString(), metadataManager, null);

            CryptoService cryptoService =
                    new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadataManager);
            keyService.setCryptoService(cryptoService);

            system = new ForwardSecureANNSystem(
                    configFile.toString(),
                    tempDir.resolve("data-not-used.csv").toString(),
                    keysDir.toString(),
                    List.of(2),
                    tempDir,
                    true,
                    metadataManager,
                    cryptoService,
                    1024
            );
            system.setExitOnShutdown(false);

            // At ctor time, CSV header should be initialized
            Path reencCsv = resultsDir.resolve("reencrypt_metrics.csv");
            assertTrue(Files.exists(reencCsv), "Re-encryption CSV should be created at init");

            // No queries issued, so touched set is empty, but shutdown should still write SUMMARY
            system.shutdown();

            List<String> lines = Files.readAllLines(reencCsv);
            assertFalse(lines.isEmpty(), "CSV must not be empty");
            assertTrue(lines.get(0).startsWith("QueryID,TargetVersion,Mode,Touched,NewUnique,"),
                    "Header must be present");

            boolean hasSummary = lines.stream().anyMatch(l -> l.startsWith("SUMMARY,"));
            assertTrue(hasSummary, "There should be a SUMMARY row in reencrypt_metrics.csv");
        } finally {
            metadataManager = null;
        }
    }
}
