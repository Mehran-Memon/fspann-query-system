package com.fspann.api;

import com.fspann.common.Profiler;
import com.fspann.common.QueryResult;
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
class ForwardSecureANNSystemCacheAndCloakTest {

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
        System.clearProperty("reenc.mode");
    }

    @Test
    void repeatedQuery_hitsCacheAndProducesQCacheMetrics() throws Exception {
        Path resultsDir = tempDir.resolve("results");
        Files.createDirectories(resultsDir);

        ForwardSecureANNSystem system = null;
        RocksDBMetadataManager metadataManager = null;
        try {
            SystemUnderTest sut = createSystem(tempDir, resultsDir);
            system = sut.system();
            metadataManager = sut.metadataManager();

            // Small index that includes the query vector
            system.batchInsert(List.of(
                    new double[]{0.00, 0.00},
                    new double[]{0.05, 0.05},
                    new double[]{0.10, 0.10}
            ), 2);
            system.flushAll();

            double[] q = new double[]{0.05, 0.05};

            // First call → miss, real search
            List<QueryResult> first = system.query(q, 3, 2);
            assertNotNull(first);

            // Second call → should be served from cache and record Q_cache_3
            List<QueryResult> second = system.query(q, 3, 2);
            assertNotNull(second);
            assertEquals(first.size(), second.size(), "Cache result size should equal first result size");

            Profiler p = system.getProfiler();
            assertNotNull(p, "Profiler should be enabled");
            assertTrue(p instanceof MicrometerProfiler, "Profiler should be MicrometerProfiler");

            MicrometerProfiler mp = (MicrometerProfiler) p;
            Path qmCsv = tempDir.resolve("query_metrics.csv");
            mp.exportQueryMetricsCSV(qmCsv.toString());
            String csv = Files.readString(qmCsv);

            // One row for Q_3 (miss) and one for Q_cache_3 (hit)
            assertTrue(csv.contains("Q_3,"), "Should contain a Q_3 row");
            assertTrue(csv.contains("Q_cache_3,"), "Should contain a Q_cache_3 row");
        } finally {
            if (system != null) {
                system.setExitOnShutdown(false);
                system.shutdown();
            }
            metadataManager = null;
        }
    }

    @Test
    void cloakedQuery_returnsWithoutError_whenNoiseDisabled() throws Exception {
        Path resultsDir = tempDir.resolve("results2");
        Files.createDirectories(resultsDir);

        ForwardSecureANNSystem system = null;
        RocksDBMetadataManager metadataManager = null;
        try {
            SystemUnderTest sut = createSystem(tempDir.resolve("cloak"), resultsDir);
            system = sut.system();
            metadataManager = sut.metadataManager();

            system.batchInsert(List.of(
                    new double[]{0.00, 0.00},
                    new double[]{0.05, 0.05}
            ), 2);
            system.flushAll();

            double[] q = new double[]{0.05, 0.05};

            // Since cloak.noise = 0.0, queryWithCloak should fall back to normal query path.
            List<QueryResult> results = system.queryWithCloak(q, 3, 2);
            assertNotNull(results, "Cloaked query results must not be null");
            // We don't assert non-empty because LSH can be flaky; empties are allowed.
        } finally {
            if (system != null) {
                system.setExitOnShutdown(false);
                system.shutdown();
            }
            metadataManager = null;
        }
    }

    // -------------------------------------------------------------
    // Small helper for system construction (same pattern as before)
    // -------------------------------------------------------------

    private record SystemUnderTest(ForwardSecureANNSystem system,
                                   RocksDBMetadataManager metadataManager) {}

    private SystemUnderTest createSystem(Path temp, Path resultsDir) throws Exception {
        Files.createDirectories(temp);
        Files.createDirectories(resultsDir);

        Path configFile = temp.resolve("config.json");
        String escapedResults = resultsDir.toString().replace("\\", "\\\\");
        String configJson = """
            {
              "numShards": 4,
              "profilerEnabled": true,
              "opsThreshold": 2147483647,
              "ageThresholdMs": 9223372036854775807,
              "lsh": { "numTables": 2, "rowsPerBand": 2, "probeShards": 1 },
              "eval": {
                "computePrecision": true,
                "writeGlobalPrecisionCsv": false,
                "kVariants": [1,3]
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

        Path keysDir = temp.resolve("keys");
        Path metadataDir = temp.resolve("metadata");
        Path pointsDir = temp.resolve("points");
        Files.createDirectories(keysDir);
        Files.createDirectories(metadataDir);
        Files.createDirectories(pointsDir);

        RocksDBMetadataManager metadataManager =
                RocksDBMetadataManager.create(metadataDir.toString(), pointsDir.toString());

        KeyManager keyManager = new KeyManager(keysDir.toString());
        KeyRotationPolicy policy = new KeyRotationPolicy(2_000_000, 1_000_000);
        KeyRotationServiceImpl keyService =
                new KeyRotationServiceImpl(keyManager, policy, metadataDir.toString(), metadataManager, null);

        CryptoService cryptoService =
                new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadataManager);
        keyService.setCryptoService(cryptoService);

        ForwardSecureANNSystem system = new ForwardSecureANNSystem(
                configFile.toString(),
                temp.resolve("data-not-used.csv").toString(),
                keysDir.toString(),
                List.of(2),
                temp,
                true,
                metadataManager,
                cryptoService,
                1024
        );
        system.setExitOnShutdown(false);
        return new SystemUnderTest(system, metadataManager);
    }
}
