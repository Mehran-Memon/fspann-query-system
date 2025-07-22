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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class ForwardSecureANNSystemIntegrationTest {

    private ForwardSecureANNSystem system;

    @BeforeEach
    void setup() {}

    @AfterEach
    void tearDown() throws InterruptedException {
        if (system != null) {
            system.shutdown();
            system = null;
        }
        System.gc();
        Thread.sleep(200);
    }

    @Test
    void simpleQueryRecallEvaluation(@TempDir Path tempDir) throws Exception {
        // Create sample dataset
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

        RocksDBMetadataManager metadataManager = new RocksDBMetadataManager(tempDir.toString());
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

        assertTrue(system.getIndexedVectorCount() > 0);

        // Groundtruth (manually created): for Q0 (close to 0.0), best is 0; for Q1, best is 2
        GroundtruthManager gt = new GroundtruthManager();
        gt.put(0, new int[]{0, 1});  // top2 for query 0
        gt.put(1, new int[]{2, 1});  // top2 for query 1

        List<double[]> queries = List.of(
                new double[]{0.05, 0.05},
                new double[]{0.95, 0.95}
        );

        for (int i = 0; i < queries.size(); i++) {
            QueryToken token = system.cloakQuery(queries.get(i), 2);
            List<QueryEvaluationResult> evals = system.getQueryService()
                    .searchWithTopKVariants(token, i, gt);

            assertFalse(evals.isEmpty(), "No evaluation results");
            for (QueryEvaluationResult r : evals) {
                assertTrue(r.getRecall() >= 0 && r.getRecall() <= 1.0);
                assertTrue(r.getRatio() >= 0);
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

        // Export results
        if (system.getProfiler() != null) {
            system.getProfiler().exportTopKVariants(tempDir.resolve("topk.csv").toString());
            assertTrue(Files.exists(tempDir.resolve("topk.csv")));
        }
    }
}
