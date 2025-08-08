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

        // Keys as a DIRECTORY (not a single .ser file)
        Path keysDir = tempDir.resolve("keys");
        Files.createDirectories(keysDir);

        // Metadata manager via factory (singleton-safe)
        Path metadataDir = tempDir.resolve("metadata");
        Path pointsDir = tempDir.resolve("points");
        Files.createDirectories(metadataDir);
        Files.createDirectories(pointsDir);
        this.metadataManager = RocksDBMetadataManager.create(metadataDir.toString(), pointsDir.toString());

        KeyManager keyManager = new KeyManager(keysDir.toString());
        KeyRotationPolicy policy = new KeyRotationPolicy(1000, 1_000_000);
        KeyRotationServiceImpl keyService = new KeyRotationServiceImpl(
                keyManager, policy, metadataDir.toString(), metadataManager, null
        );

        CryptoService cryptoService = new AesGcmCryptoService(
                new SimpleMeterRegistry(), keyService, metadataManager
        );
        keyService.setCryptoService(cryptoService);

        this.system = new ForwardSecureANNSystem(
                config.toString(),
                dataFile.toString(),
                keysDir.toString(),
                List.of(2),
                tempDir,
                true,
                metadataManager,
                cryptoService,
                10
        );
        this.system.setExitOnShutdown(false); // prevent System.exit in tests

        assertNotNull(system.getIndexService(), "IndexService should not be null");
        assertNotNull(system.getQueryService(), "QueryService should not be null");
    }

    @AfterEach
    void tearDown() {
        if (system != null) {
            system.shutdown(); // this also closes metadataManager
            system = null;
        }
        metadataManager = null; // already closed by system
        // Let @TempDir cleanup the directory tree automatically.
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
    void testQueryWithCloakReturnsResults() {
        double[] query = new double[]{0.05, 0.05};
        List<QueryResult> results = system.queryWithCloak(query, 2, 2);
        assertNotNull(results);
        assertFalse(results.isEmpty());
    }

    @Test
    void testFakePointInsertionIncreasesCount() {
        int before = system.getIndexedVectorCount();
        system.insertFakePointsInBatches(5, 2);
        int after = system.getIndexedVectorCount();
        assertEquals(before + 5, after);
    }
}
