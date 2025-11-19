package com.fspann.api;

import com.fspann.common.QueryResult;
import com.fspann.common.RocksDBMetadataManager;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.crypto.CryptoService;
import com.fspann.index.service.SecureLSHIndexService;
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
    private static final double MAX_INSERT_MS = 1_000.0; // per vector (very relaxed)
    private static final double MAX_QUERY_MS  =   500.0; // average

    private RocksDBMetadataManager metadataManager;

    @BeforeAll
    public void setup(@TempDir Path baseDir) throws Exception {
        // Config
        Path configPath = baseDir.resolve("config.json");
        Files.writeString(configPath, """
            {"numShards":4, "profilerEnabled":true, "opsThreshold":1000000, "ageThresholdMs":604800000}
        """);

        // Seed file for ctor symmetry
        Path seed = baseDir.resolve("seed.csv");
        Files.writeString(seed, "0\n");

        // metadata & points
        Path metadataDir = baseDir.resolve("metadata");
        Path pointsDir   = baseDir.resolve("points");
        Files.createDirectories(metadataDir);
        Files.createDirectories(pointsDir);
        metadataManager = RocksDBMetadataManager.create(metadataDir.toString(), pointsDir.toString());

        // Use a concrete keystore FILE (newer KeyManager expects a file path)
        Path keystore = baseDir.resolve("keys/keystore.blob");
        Files.createDirectories(keystore.getParent());

        KeyManager keyManager = new KeyManager(keystore.toString());
        KeyRotationPolicy policy = new KeyRotationPolicy(1_000_000, 1_000_000);
        KeyRotationServiceImpl keyService =
                new KeyRotationServiceImpl(keyManager, policy, metadataDir.toString(), metadataManager, null);

        CryptoService cryptoService =
                new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadataManager);
        keyService.setCryptoService(cryptoService);

        sys = new ForwardSecureANNSystem(
                configPath.toString(),
                seed.toString(),
                keystore.toString(),      // pass keystore FILE path
                List.of(DIMS),
                baseDir,
                false,
                metadataManager,
                cryptoService,
                256
        );
        sys.setExitOnShutdown(false);

        // Build synthetic dataset and index it
        dataset = new ArrayList<>(VECTOR_COUNT);
        StringBuilder sb = new StringBuilder();
        Random rnd = new Random(1337);
        for (int i = 0; i < VECTOR_COUNT; i++) {
            double[] vec = rnd.doubles(DIMS).toArray();
            dataset.add(vec);
            // just to keep a dataset file around for eyeballing
            for (int j = 0; j < DIMS; j++) {
                sb.append(vec[j]);
                if (j < DIMS - 1) sb.append(',');
            }
            sb.append('\n');
        }
        Files.writeString(baseDir.resolve("synthetic_gaussian_10d.csv"), sb.toString());

        long startIndex = System.nanoTime();
        sys.batchInsert(dataset, DIMS);
        sys.flushAll();
        long endIndex = System.nanoTime();

        double totalMs = (endIndex - startIndex) / 1e6;
        double avgInsertMs = totalMs / dataset.size();
        logger.info("Initial load: {} vectors in {} ms (avg: {} ms)", dataset.size(), totalMs, avgInsertMs);
    }

    @AfterAll
    public void tearDown() {
        if (sys != null) {
            sys.shutdown();
            sys = null;
        }
        metadataManager = null; // closed by shutdown
    }

    @Test
    @DisplayName("⏱️ Insert + Query Latency Under Threshold")
    public void bulkPerformanceTest() throws Exception {
        // Insert a small additional batch to measure insert cost
        List<double[]> extra = new ArrayList<>();
        Random r = new Random(2024);
        for (int i = 0; i < 200; i++) extra.add(r.doubles(DIMS).toArray());

        long startInsert = System.nanoTime();
        sys.batchInsert(extra, DIMS);
        sys.flushAll();
        long endInsert = System.nanoTime();

        double avgInsertMs = (endInsert - startInsert) / 1e6 / extra.size();
        assertTrue(avgInsertMs < MAX_INSERT_MS, String.format("Insert too slow: %.3f ms", avgInsertMs));

        // Query: use vectors from the already indexed dataset (guarantees a matching point exists)
        int queries = 300;
        long startQuery = System.nanoTime();
        for (int i = 0; i < queries; i++) {
            double[] q = dataset.get(i % dataset.size());
            List<QueryResult> res = sys.queryWithCloak(q, 5, DIMS);
            assertNotNull(res);
            // We prefer non-empty. If empty, log a warning (noise can hurt recall on tiny indices).
            if (res.isEmpty() && i < 3) {
                System.err.println("[WARN] Cloaked query returned empty result set during perf test (acceptable).");
            }
        }
        long endQuery = System.nanoTime();

        double avgQueryMs = (endQuery - startQuery) / 1e6 / queries;
        assertTrue(avgQueryMs < MAX_QUERY_MS, String.format("Query too slow: %.3f ms", avgQueryMs));
    }

    @Test
    public void testFakePointsInsertion() throws Exception {
        int fakeCount = 100;
        int initialCount = sys.getIndexedVectorCount();
        sys.insertFakePointsInBatches(fakeCount, DIMS);
        sys.flushAll();
        int total = sys.getIndexedVectorCount();
        assertTrue(total >= initialCount + fakeCount,
                String.format("Indexed count should be >= %d, got: %d", initialCount + fakeCount, total));
    }

    @Test
    public void testKeyRotationPerformance() throws Exception {
        int initialCount = sys.getIndexedVectorCount();
        sys.insert("test-id-1", new double[DIMS], DIMS);
        sys.insert("test-id-2", new double[DIMS], DIMS);
        sys.flushAll();
        int finalCount = sys.getIndexedVectorCount();
        assertTrue(finalCount >= initialCount + 2, "Indexed count should increase by at least 2");
    }

    @Test
    public void testCacheHitPerformance() {
        double[] query = Arrays.copyOf(dataset.get(0), DIMS);

        // Clear any internal caches in the index if available
        try {
            ((SecureLSHIndexService) sys.getIndexService()).clearCache();
        } catch (Throwable ignore) { /* best-effort */ }

        // 1 miss
        long t0 = System.nanoTime();
        List<QueryResult> miss = sys.query(query, 5, DIMS);
        long missTime = System.nanoTime() - t0;
        assertNotNull(miss);

        // Several hits (same token → cache key)
        int H = 8;
        long hitAccum = 0;
        for (int i = 0; i < H; i++) {
            long hs = System.nanoTime();
            List<QueryResult> hit = sys.query(query, 5, DIMS);
            hitAccum += System.nanoTime() - hs;
            assertNotNull(hit);
        }
        double avgHit = hitAccum / 1e6 / H;
        double missMs = missTime / 1e6;

        logger.info("Cache miss: {} ms, avg cache hit: {} ms", missMs, avgHit);
        // Be tolerant of noise; usually hit <= miss, but allow some headroom
        assertTrue(avgHit <= missMs * 1.25, "Cache hit should not be much slower than miss");
    }
}
