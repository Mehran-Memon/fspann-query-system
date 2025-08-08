package com.fspann.api;

import com.fspann.common.*;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.crypto.CryptoService;
import com.fspann.key.KeyManager;
import com.fspann.key.KeyRotationPolicy;
import com.fspann.key.KeyRotationServiceImpl;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ForwardSecureANNSystemPerformanceIntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(ForwardSecureANNSystemPerformanceIntegrationTest.class);

    private ForwardSecureANNSystem sys;
    private List<double[]> dataset;
    private static final int DIMS = 10;
    private static final int VECTOR_COUNT = 1000;
    private static final double MAX_INSERT_MS = 1000.0;
    private static final double MAX_QUERY_MS = 500.0;

    private RocksDBMetadataManager metadataManager;
    private Path baseDir;

    @BeforeAll
    public void setup(@TempDir Path tempDir) throws Exception {
        this.baseDir = tempDir;
        Path configPath = baseDir.resolve("config.json");
        Files.writeString(configPath, "{\"numShards\":4, \"profilerEnabled\":true, \"opsThreshold\":2, \"ageThresholdMs\":1000}");
        logger.debug("Created config: {}", configPath);

        dataset = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < VECTOR_COUNT; i++) {
            double[] vec = new double[DIMS];
            for (int j = 0; j < DIMS; j++) {
                vec[j] = Math.random();
                sb.append(vec[j]);
                if (j < DIMS - 1) sb.append(',');
            }
            sb.append('\n');
            dataset.add(vec);
        }

        Path dataPath = baseDir.resolve("synthetic_gaussian_10d.csv");
        Files.writeString(dataPath, sb.toString());
        logger.debug("Generated data file: {} with {} vectors", dataPath, dataset.size());

        Path metadataDir = baseDir.resolve("metadata");
        Path pointsDir = baseDir.resolve("points");
        Files.createDirectories(metadataDir);
        Files.createDirectories(pointsDir);
        metadataManager = RocksDBMetadataManager.create(metadataDir.toString(), pointsDir.toString());

        Path keysDir = baseDir.resolve("keys");
        Files.createDirectories(keysDir);

        KeyManager keyManager = new KeyManager(keysDir.toString());
        KeyRotationPolicy policy = new KeyRotationPolicy(100, Long.MAX_VALUE);
        KeyRotationServiceImpl keyService = new KeyRotationServiceImpl(keyManager, policy, metadataDir.toString(), metadataManager, null);
        CryptoService cryptoService = new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadataManager);
        keyService.setCryptoService(cryptoService);

        sys = new ForwardSecureANNSystem(
                configPath.toString(),
                dataPath.toString(),
                keysDir.toString(),
                List.of(DIMS),
                baseDir,
                false,
                metadataManager,
                cryptoService,
                100
        );
        sys.setExitOnShutdown(false);
        logger.info("🚀 System initialized successfully");
    }

    @AfterAll
    public void tearDown() {
        if (sys != null) {
            sys.shutdown();
            sys = null;
        }
        metadataManager = null; // closed by system.shutdown()
        // Let @TempDir clean up.
    }

    @Test
    @DisplayName("⏱️ Performance Test: Insert + Query Latency Under Threshold")
    public void bulkPerformanceTest() {
        assertNotNull(dataset, "Dataset must be initialized");
        assertEquals(VECTOR_COUNT, dataset.size(), "Dataset size mismatch");

        long startInsert = System.nanoTime();
        sys.batchInsert(dataset, DIMS);
        long endInsert = System.nanoTime();

        double totalMs = (endInsert - startInsert) / 1e6;
        double avgInsertMs = totalMs / dataset.size();
        logger.info("Inserted {} vectors in {} ms (avg: {} ms)", dataset.size(), totalMs, avgInsertMs);
        assertTrue(avgInsertMs < MAX_INSERT_MS, String.format("⚠️ Insert too slow: %.3f ms", avgInsertMs));

        int queries = 200;
        long startQuery = System.nanoTime();
        for (int i = 0; i < queries; i++) {
            double[] query = dataset.get(i % dataset.size());
            sys.queryWithCloak(query, 5, DIMS);
        }
        long endQuery = System.nanoTime();

        double avgQueryMs = (endQuery - startQuery) / 1e6 / queries;
        logger.info("Average query latency: {} ms", avgQueryMs);
        assertTrue(avgQueryMs < MAX_QUERY_MS, String.format("⚠️ Query too slow: %.3f ms", avgQueryMs));
    }

    @Test
    public void testFakePointsInsertion() {
        int fakeCount = 100;
        int initialCount = sys.getIndexedVectorCount();
        sys.insertFakePointsInBatches(fakeCount, DIMS);
        int total = sys.getIndexedVectorCount();
        logger.info("Total indexed vectors after fake insert: {}", total);
        assertEquals(initialCount + fakeCount, total,
                String.format("Indexed count should be %d, got: %d", initialCount + fakeCount, total));
    }

    @Test
    public void testKeyRotationPerformance() {
        int initialCount = sys.getIndexedVectorCount();
        sys.insert("test-id", new double[DIMS], DIMS); // Trigger rotation counts
        sys.insert("test-id2", new double[DIMS], DIMS); // Trigger rotation counts
        int finalCount = sys.getIndexedVectorCount();
        assertEquals(initialCount + 2, finalCount, "Indexed count should increase by 2");
    }

    @Test
    public void testCacheHitPerformance() {
        double[] query = new double[DIMS];
        Arrays.fill(query, 0.5);
        long start = System.nanoTime();
        sys.query(query, 5, DIMS); // Cache miss
        long missTime = System.nanoTime() - start;
        start = System.nanoTime();
        sys.query(query, 5, DIMS); // Cache hit
        long hitTime = System.nanoTime() - start;
        logger.info("Cache miss: {} ms, Cache hit: {} ms", missTime / 1e6, hitTime / 1e6);
        assertTrue(hitTime < missTime, "Cache hit should be faster than miss");
    }
}
