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

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ForwardSecureANNSystemFanoutInternalTest {

    @TempDir
    Path tempDir;

    @BeforeAll
    static void enableTestMode() {
        System.setProperty("test.env", "true");
    }

    @AfterEach
    void clearProps() {
        System.clearProperty("results.dir");
        System.clearProperty("probe.shards");
    }

    @Test
    void computeAndLogFanoutForQ0_returnsPerKMap() throws Exception {
        Path resultsDir = tempDir.resolve("results");
        Files.createDirectories(resultsDir);

        ForwardSecureANNSystem system = null;
        RocksDBMetadataManager metadataManager = null;
        try {
            // Build tiny system
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
                "kVariants": [1,4,8]
              },
              "cloak": { "noise": 0.0 },
              "ratio": {
                "source": "base",
                "gtSample": 0,
                "gtMismatchTolerance": 1.0
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

            metadataManager =
                    RocksDBMetadataManager.create(metadataDir.toString(), pointsDir.toString());
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

            // Index a few simple points
            system.batchInsert(List.of(
                    new double[]{0.0, 0.0},
                    new double[]{0.1, 0.1},
                    new double[]{0.2, 0.2},
                    new double[]{0.3, 0.3},
                    new double[]{0.4, 0.4}
            ), 2);
            system.flushAll();
            system.finalizeForSearch();

            // Reflective call to computeAndLogFanoutForQ0
            Method m = ForwardSecureANNSystem.class.getDeclaredMethod(
                    "computeAndLogFanoutForQ0",
                    double[].class,
                    int.class,
                    int[].class
            );
            m.setAccessible(true);

            double[] q0 = new double[]{0.1, 0.1};
            int[] kVariants = new int[]{1, 4, 8};

            @SuppressWarnings("unchecked")
            Map<Integer, Double> fanout = (Map<Integer, Double>) m.invoke(system, q0, 2, kVariants);

            assertNotNull(fanout, "Fanout map must not be null");
            assertEquals(kVariants.length, fanout.size(), "Map should have an entry for each K variant");

            // At least one K should have a finite fanout value.
            boolean someFinite = fanout.values().stream().anyMatch(Double::isFinite);
            assertTrue(someFinite, "At least one finite fanout value expected");
        } finally {
            if (system != null) {
                system.setExitOnShutdown(false);
                system.shutdown();
            }
            metadataManager = null;
        }
    }
}
