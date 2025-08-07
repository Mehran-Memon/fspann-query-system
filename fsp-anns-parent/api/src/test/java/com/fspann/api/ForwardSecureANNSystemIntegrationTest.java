package com.fspann.api;

import com.fspann.common.*;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.crypto.CryptoService;
import com.fspann.key.KeyManager;
import com.fspann.key.KeyRotationPolicy;
import com.fspann.key.KeyRotationServiceImpl;
import com.fspann.loader.GroundtruthManager;
import com.fspann.query.core.QueryEvaluationResult;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ForwardSecureANNSystemIntegrationTest {

    private ForwardSecureANNSystem system;
    private RocksDBMetadataManager metadataManager;
    private Path tempDir;

    @BeforeAll
    static void enableTestMode() {
        System.setProperty("test.env", "true");
    }

    @BeforeEach
    void setup(@TempDir Path tempDir) throws Exception {
        this.tempDir = tempDir;

        Path dataFile = tempDir.resolve("data.csv");
        Files.writeString(dataFile, "0.1,0.1\n0.5,0.5\n");

        Path queryFile = tempDir.resolve("query.csv");
        Files.writeString(queryFile, "0.1,0.1\n");

        Path config = tempDir.resolve("config.json");
        Files.writeString(config, """
            {
              "numShards": 2,
              "profilerEnabled": true,
              "opsThreshold": 1000,
              "ageThresholdMs": 1000000
            }
        """);

        Path keys = tempDir.resolve("keys.ser");
        List<Integer> dimensions = List.of(2);

        this.metadataManager = new RocksDBMetadataManager(
                tempDir.resolve("metadata").toString(),
                tempDir.resolve("points").toString()
        );

        KeyManager keyManager = new KeyManager(keys.toString());
        KeyRotationPolicy policy = new KeyRotationPolicy(1000, 1000000);
        KeyRotationServiceImpl keyService = new KeyRotationServiceImpl(
                keyManager, policy, tempDir.resolve("metadata").toString(), metadataManager, null
        );

        CryptoService cryptoService = new AesGcmCryptoService(
                new SimpleMeterRegistry(), keyService, metadataManager
        );
        keyService.setCryptoService(cryptoService);

        this.system = new ForwardSecureANNSystem(
                config.toString(),
                dataFile.toString(),
                keys.toString(),
                dimensions,
                tempDir,
                true,
                metadataManager,
                cryptoService,
                10
        );

        assertNotNull(system.getIndexService(), "IndexService should not be null");
        assertNotNull(system.getQueryService(), "QueryService should not be null");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (system != null) {
            try {
                system.shutdown(); // Must call this before metadataManager.close()
            } catch (Exception e) {
                System.err.println("Failed to shutdown system: " + e.getMessage());
            }
            system = null;
        }
        if (metadataManager != null) {
            try {
                RocksDBTestCleaner.clean(metadataManager); // Call BEFORE close()
                metadataManager.close();
            } catch (Exception e) {
                System.err.println("Failed to close metadata manager: " + e.getMessage());
            }
            metadataManager = null;
        }

        // Retry deleting the temp directory to handle Windows file locking
        int maxRetries = 5;
        for (int i = 0; i < maxRetries; i++) {
            try (Stream<Path> walk = Files.walk(tempDir)) {
                walk.sorted(Comparator.reverseOrder()).forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException e) {
                        System.err.println("Failed to delete " + path + ": " + e.getMessage());
                    }
                });
                break; // If successful, exit the retry loop
            } catch (IOException e) {
                System.err.println("Retry " + (i + 1) + " failed to delete temp directory: " + e.getMessage());
                if (i < maxRetries - 1) {
                    Thread.sleep(500); // Wait before retrying
                } else {
                    System.err.println("Failed to delete temp directory after " + maxRetries + " attempts");
                }
            }
        }

        // Force GC to clean native handles
        System.gc();
        Thread.sleep(500); // Increased delay to ensure native resources are released
    }

    @Test
    void testQueryRecallEvaluation() throws Exception {
        assertTrue(system.getIndexedVectorCount() > 0, "Should have some indexed vectors");

        GroundtruthManager gt = new GroundtruthManager();
        gt.put(0, new int[]{0, 1});
        gt.put(1, new int[]{1});

        List<double[]> queries = List.of(
                new double[]{0.1, 0.1},
                new double[]{0.9, 0.9}
        );

        for (int i = 0; i < queries.size(); i++) {
            QueryToken token = system.cloakQuery(queries.get(i), 2, 2);
            List<QueryEvaluationResult> evals = system.getQueryService()
                    .searchWithTopKVariants(token, i, gt);

            assertFalse(evals.isEmpty(), "Should have evaluation results");
            for (QueryEvaluationResult r : evals) {
                assertTrue(r.getRecall() >= 0 && r.getRecall() <= 1.0);
                assertTrue(r.getRatio() >= 0);
            }
        }
    }

    @Test
    void testQueryWithCloakReturnsResults() throws Exception {
        double[] query = new double[]{0.05, 0.05};
        List<QueryResult> results = system.queryWithCloak(query, 2, 2);
        assertNotNull(results);
        assertFalse(results.isEmpty());
    }

    @Test
    void testFakePointInsertionIncreasesCount() throws Exception {
        int before = system.getIndexedVectorCount();
        system.insertFakePointsInBatches(5, 2);
        int after = system.getIndexedVectorCount();
        assertEquals(before + 5, after);
    }
}
