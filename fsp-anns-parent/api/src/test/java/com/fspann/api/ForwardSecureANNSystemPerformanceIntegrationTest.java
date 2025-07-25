package com.fspann.api;

import com.fspann.common.*;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.crypto.CryptoService;
import com.fspann.key.KeyManager;
import com.fspann.key.KeyRotationPolicy;
import com.fspann.key.KeyRotationServiceImpl;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class ForwardSecureANNSystemPerformanceIntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(ForwardSecureANNSystemPerformanceIntegrationTest.class);
    private static ForwardSecureANNSystem sys;
    private static List<double[]> dataset;
    private static final int DIMS = 10;
    private static final int VECTOR_COUNT = 1000;
    private static final double MAX_INSERT_MS = 1000.0;
    private static final double MAX_QUERY_MS = 500.0;

    @BeforeAll
    public static void setup(@TempDir Path tempDir) throws Exception {
        Path configPath = tempDir.resolve("config.json");
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

        Path dataPath = tempDir.resolve("synthetic_gaussian_128d_storage.csv");
        Files.writeString(dataPath, sb.toString());
        logger.debug("Generated data file: {} with {} vectors", dataPath, dataset.size());

        Path keysPath = tempDir.resolve("keys.ser");
        RocksDBMetadataManager metadataManager = new RocksDBMetadataManager(tempDir.toString());

        KeyManager keyManager = new KeyManager(keysPath.toString());
        KeyRotationPolicy policy = new KeyRotationPolicy(2, 1000);
        KeyRotationServiceImpl keyService = new KeyRotationServiceImpl(keyManager, policy, tempDir.toString(), metadataManager, null);
        CryptoService cryptoService = new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadataManager);
        keyService.setCryptoService(cryptoService);

        sys = new ForwardSecureANNSystem(
                configPath.toString(),
                dataPath.toString(),
                keysPath.toString(),
                List.of(DIMS),
                tempDir,
                false,
                metadataManager,
                cryptoService,
                100
        );

        logger.info("🚀 System initialized successfully");
    }

    @AfterAll
    public static void tearDown(@TempDir Path tempDir) throws Exception {
        if (sys != null) {
            logger.info("🧹 Shutting down system");
            sys.shutdown();
            sys = null;
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
        System.out.println("Performance integration test completed");
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
    public void testKeyRotationPerformance() throws Exception {
        int initialCount = sys.getIndexedVectorCount();
        sys.insert("test-id", new double[DIMS], DIMS); // Trigger rotation
        sys.insert("test-id2", new double[DIMS], DIMS); // Trigger rotation
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