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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class ForwardSecureANNSystemIntegrationTest {
    private ForwardSecureANNSystem system;

    @BeforeEach
    void setup(@TempDir Path tempDir) throws Exception {
        Path dataFile = tempDir.resolve("data.csv");
        Files.writeString(dataFile, "0.0,0.0\n0.1,0.1\n0.9,0.9\n");

        Path queryFile = tempDir.resolve("query.csv");
        Files.writeString(queryFile, "0.05,0.05\n0.95,0.95");

        Path config = tempDir.resolve("config.json");
        Files.writeString(config, """
            {
              "numShards": 4,
              "profilerEnabled": true,
              "opsThreshold": 10000,
              "ageThresholdMs": 9999999
            }
        """);

        Path keys = tempDir.resolve("keys.ser");
        List<Integer> dimensions = List.of(2);
        RocksDBMetadataManager metadataManager = new RocksDBMetadataManager(tempDir.toString(), tempDir.resolve("points").toString());
        KeyManager keyManager = new KeyManager(keys.toString());
        KeyRotationPolicy policy = new KeyRotationPolicy(100, 10000);
        KeyRotationServiceImpl keyService = new KeyRotationServiceImpl(keyManager, policy, tempDir.toString(), metadataManager, null);
        CryptoService cryptoService = new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadataManager);
        keyService.setCryptoService(cryptoService);

        system = new ForwardSecureANNSystem(
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
        assertNotNull(system.getIndexService(), "IndexService should be initialized");
        assertNotNull(system.getQueryService(), "QueryService should be initialized");
    }

    @AfterEach
    void tearDown(@TempDir Path tempDir) throws Exception {
        if (system != null) {
            system.shutdown();
            system = null;
        }
        Files.walk(tempDir)
                .sorted(Comparator.reverseOrder())
                .forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException e) {
                        System.err.println("Failed to delete " + path);
                    }
                });
        System.gc();
    }

    @Test
    void simpleQueryRecallEvaluation(@TempDir Path tempDir) throws Exception {
        assertTrue(system.getIndexedVectorCount() > 0, "Indexed vector count should be positive");

        GroundtruthManager gt = new GroundtruthManager();
        gt.put(0, new int[]{0, 1});
        gt.put(1, new int[]{2, 1});

        List<double[]> queries = List.of(
                new double[]{0.05, 0.05},
                new double[]{0.95, 0.95}
        );

        for (int i = 0; i < queries.size(); i++) {
            QueryToken token = system.cloakQuery(queries.get(i), 2, 2);
            List<QueryEvaluationResult> evals = system.getQueryService()
                    .searchWithTopKVariants(token, i, gt);

            assertFalse(evals.isEmpty(), "No evaluation results");
            for (QueryEvaluationResult r : evals) {
                assertTrue(r.getRecall() >= 0 && r.getRecall() <= 1.0, "Recall out of range");
                assertTrue(r.getRatio() >= 0, "Ratio should be non-negative");
            }
            if (system.getProfiler() != null) {
                for (QueryEvaluationResult r : evals) {
                    system.getProfiler().recordTopKVariants(
                            "Q" + i,
                            r.getTopKRequested(),
                            r.getRetrieved(),
                            r.getRatio(),
                            r.getRecall(),
                            r.getTimeMs()
                    );
                }
            }
        }

        if (system.getProfiler() != null) {
            system.getProfiler().exportTopKVariants(tempDir.resolve("topk.csv").toString());
            assertTrue(Files.exists(tempDir.resolve("topk.csv")), "TopK CSV should exist");
        }
    }

    @Test
    void testQueryWithCloak(@TempDir Path tempDir) throws Exception {
        double[] query = new double[]{0.05, 0.05};
        List<QueryResult> results = system.queryWithCloak(query, 2, 2);
        assertNotNull(results, "Query results should not be null");
        assertFalse(results.isEmpty(), "Query results should not be empty");
    }

    @Test
    void testInsertFakePoints(@TempDir Path tempDir) throws Exception {
        int fakeCount = 10;
        int initialCount = system.getIndexedVectorCount();
        system.insertFakePointsInBatches(fakeCount, 2);
        assertEquals(initialCount + fakeCount, system.getIndexedVectorCount(),
                "Indexed vector count should increase by fakeCount");
    }
}