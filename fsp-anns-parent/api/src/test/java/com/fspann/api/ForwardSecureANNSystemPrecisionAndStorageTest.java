package com.fspann.api;

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

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ForwardSecureANNSystemPrecisionAndStorageTest {

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

    // -------------------------------------------------------------
    // 1) Direct test of writeGlobalPrecisionCsv via reflection
    // -------------------------------------------------------------
    @Test
    void globalPrecisionCsv_reflectiveHelper_writesExpectedValues() throws Exception {
        Path outDir = tempDir.resolve("gp");
        Files.createDirectories(outDir);

        int dim = 128;
        int[] kVariants = new int[]{1, 10};

        Map<Integer, Long> matches = new HashMap<>();
        Map<Integer, Long> retrieved = new HashMap<>();

        // Suppose we had 5 queries:
        // For K=1: each query returned exactly 1 item; 4/5 were correct
        matches.put(1, 4L);
        retrieved.put(1, 5L);

        // For K=10: over 5 queries we returned exactly 10 per query (50 total);
        // 35 of these were correct (precision = 35/50 = 0.7)
        matches.put(10, 35L);
        retrieved.put(10, 50L);

        int queriesCount = 5;

        Method m = ForwardSecureANNSystem.class.getDeclaredMethod(
                "writeGlobalPrecisionCsv",
                Path.class,
                int.class,
                int[].class,
                Map.class,
                Map.class,
                int.class
        );
        m.setAccessible(true);
        m.invoke(null, outDir, dim, kVariants, matches, retrieved, queriesCount);

        Path csv = outDir.resolve("global_precision.csv");
        assertTrue(Files.exists(csv), "global_precision.csv should be created");

        List<String> lines = Files.readAllLines(csv);
        assertFalse(lines.isEmpty(), "CSV must not be empty");
        assertEquals("dimension,topK,precision,return_rate,matches_sum,returned_sum,queries", lines.get(0));

        // We expect 2 data rows: K=1 and K=10
        assertEquals(3, lines.size(), "Header + 2 lines expected");

        String lineK1 = lines.get(1);
        String lineK10 = lines.get(2);

        // Simple sanity checks on numeric content
        assertTrue(lineK1.startsWith("128,1,"), "First data row should be for K=1");
        assertTrue(lineK10.startsWith("128,10,"), "Second data row should be for K=10");

        // Parse K=1 row: precision = 4 / 5 = 0.8, return_rate = 5 / (5*1) = 1.0
        String[] parts1 = lineK1.split(",");
        assertEquals("128", parts1[0]);
        assertEquals("1", parts1[1]);
        double prec1 = Double.parseDouble(parts1[2]);
        double rr1 = Double.parseDouble(parts1[3]);
        assertEquals(0.8, prec1, 1e-6);
        assertEquals(1.0, rr1, 1e-6);
        assertEquals("4", parts1[4]);   // matches
        assertEquals("5", parts1[5]);   // returned
        assertEquals("5", parts1[6]);   // queries

        // Parse K=10 row: precision = 35/50 = 0.7, return_rate = 50 / (5*10) = 1.0
        String[] parts10 = lineK10.split(",");
        assertEquals("128", parts10[0]);
        assertEquals("10", parts10[1]);
        double prec10 = Double.parseDouble(parts10[2]);
        double rr10 = Double.parseDouble(parts10[3]);
        assertEquals(0.7, prec10, 1e-6);
        assertEquals(1.0, rr10, 1e-6);
        assertEquals("35", parts10[4]);
        assertEquals("50", parts10[5]);
        assertEquals("5", parts10[6]);
    }

    // -------------------------------------------------------------
    // 2) Storage summary + breakdown via exportArtifacts
    // -------------------------------------------------------------
    @Test
    void exportArtifacts_writesStorageSummaryAndBreakdown() throws Exception {
        Path resultsDir = tempDir.resolve("results");
        Files.createDirectories(resultsDir);

        ForwardSecureANNSystem system = null;
        RocksDBMetadataManager metadataManager = null;
        try {
            // Build small system and index a few points
            SystemUnderTest sut = createSystem(tempDir, resultsDir);
            system = sut.system();
            metadataManager = sut.metadataManager();

            system.batchInsert(List.of(
                    new double[]{0.0, 0.0},
                    new double[]{0.1, 0.1},
                    new double[]{0.2, 0.2}
            ), 2);
            system.flushAll();

            Path exportDir = tempDir.resolve("artifacts");
            system.exportArtifacts(exportDir);

            Path storageCsv = exportDir.resolve("storage_summary.csv");
            Path breakdownTxt = exportDir.resolve("storage_breakdown.txt");

            assertTrue(Files.exists(storageCsv), "storage_summary.csv should exist");
            assertTrue(Files.exists(breakdownTxt), "storage_breakdown.txt should exist");

            List<String> csvLines = Files.readAllLines(storageCsv);
            assertFalse(csvLines.isEmpty(), "storage_summary.csv must not be empty");
            assertTrue(csvLines.get(0).startsWith("mode,version,dataset_size,dim,"),
                    "First line should be CSV header");

            // At least one data row
            assertTrue(csvLines.size() >= 2, "Expected at least one summary row");

            String breakdown = Files.readString(breakdownTxt);
            assertTrue(breakdown.contains("=== Storage Breakdown ==="),
                    "Breakdown file should have a human-readable header");
            assertTrue(breakdown.contains("Dataset Size (N)"),
                    "Breakdown should mention dataset size");
        } finally {
            if (system != null) {
                system.setExitOnShutdown(false);
                system.shutdown();
            }
            metadataManager = null; // closed by shutdown
        }
    }

    // -------------------------------------------------------------
    // Shared helper to build a small ForwardSecureANNSystem
    // -------------------------------------------------------------

    private record SystemUnderTest(ForwardSecureANNSystem system,
                                   RocksDBMetadataManager metadataManager) {}

    private SystemUnderTest createSystem(Path temp,
                                         Path resultsDir) throws Exception {

        // Write dynamic config.json including fully qualified resultsDir
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
                "writeGlobalPrecisionCsv": true,
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
                List.of(2),          // dimension list
                temp,
                true,                // verbose
                metadataManager,
                cryptoService,
                1024                 // batch size
        );
        system.setExitOnShutdown(false);

        return new SystemUnderTest(system, metadataManager);
    }
}
