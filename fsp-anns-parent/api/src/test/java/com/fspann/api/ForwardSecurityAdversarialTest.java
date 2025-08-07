package com.fspann.api;

import com.fspann.common.*;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.crypto.CryptoService;
import com.fspann.crypto.KeyUtils;
import com.fspann.index.service.SecureLSHIndexService;
import com.fspann.key.KeyManager;
import com.fspann.key.KeyRotationPolicy;
import com.fspann.key.KeyRotationServiceImpl;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class ForwardSecurityAdversarialTest {
    private static final int[] TEST_DIMENSIONS = {3, 128, 1024};
    private static final int NUM_POINTS = 1000;
    private static final int TOP_K = 20;
    private RocksDBMetadataManager metadataManager;

    private RocksDBMetadataManager createMetadataManager(Path baseDir) throws IOException {
        return new RocksDBMetadataManager(baseDir.toString(), baseDir.resolve("points").toString());
    }

    private String generateDummyData(int dim) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < NUM_POINTS; i++) {
            sb.append("0.0".repeat(Math.max(0, dim)).replaceAll(".$", "\n"));
        }
        return sb.toString();
    }

    @BeforeEach
    public void setupEach(@TempDir Path tempDir) throws IOException {
        this.metadataManager = new RocksDBMetadataManager(tempDir.toString(), tempDir.resolve("points").toString());
    }

    @BeforeEach
    public void prepareEmptyMetadataDir(@TempDir Path tempDir) throws IOException {
        Path baseDir = tempDir.resolve("points");
        if (Files.exists(baseDir)) {
            Files.walk(baseDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(f -> {
                        if (!f.delete()) {
                            System.err.println("Failed to delete " + f.getAbsolutePath());
                        }
                    });
        }
        Files.createDirectories(baseDir);
    }

    @Test
    public void testForwardSecurityAgainstKeyCompromise(@TempDir Path tempDir) throws Exception {
        List<Double> queryLatencies = new ArrayList<>();

        for (int dim : TEST_DIMENSIONS) {
            RocksDBMetadataManager metadataManager = createMetadataManager(tempDir);

            Path config = tempDir.resolve("config_" + dim + ".json");
            Files.writeString(config, "{\"numShards\":32,\"profilerEnabled\":true,\"opsThreshold\":999999,\"ageThresholdMs\":999999}");
            Path dummyData = tempDir.resolve("dummy_" + dim + ".csv");
            Files.writeString(dummyData, generateDummyData(dim));
            Path keys = tempDir.resolve("keys_" + dim + ".ser");

            KeyManager keyManager = new KeyManager(keys.toString());
            KeyRotationPolicy policy = new KeyRotationPolicy(999999, 999999);
            KeyRotationServiceImpl keyService = new KeyRotationServiceImpl(keyManager, policy, tempDir.toString(), metadataManager, null);
            CryptoService cryptoService = new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadataManager);
            keyService.setCryptoService(cryptoService);

            ForwardSecureANNSystem system = new ForwardSecureANNSystem(
                    config.toString(), dummyData.toString(), keys.toString(),
                    Collections.singletonList(dim), tempDir, true, metadataManager, cryptoService, 1
            );
            keyService.setIndexService(system.getIndexService());

            List<String> ids = new ArrayList<>();
            List<double[]> points = new ArrayList<>();
            Random rand = new Random();
            for (int i = 0; i < NUM_POINTS; i++) {
                String id = UUID.randomUUID().toString();
                double[] point = rand.doubles(dim).toArray();
                ids.add(id);
                points.add(point);
            }

            long insertStart = System.nanoTime();
            for (int i = 0; i < NUM_POINTS; i++) {
                system.insert(ids.get(i), points.get(i), dim);
            }
            system.flushAll();
            long insertDuration = (System.nanoTime() - insertStart) / 1_000_000;
            PerformanceVisualizer.visualizeTimings(List.of(insertDuration));

            SecretKey compromisedKey = KeyUtils.fromBytes(keyService.getCurrentVersion().getKey().getEncoded());

            keyService.rotateKey();
            ((SecureLSHIndexService) system.getIndexService()).clearCache();

            double[] pointAfter = rand.doubles(dim).toArray();
            system.insert(UUID.randomUUID().toString(), pointAfter, dim);
            system.flushAll();

            int expectedVersion = keyService.getCurrentVersion().getVersion();

            for (String id : ids) {
                EncryptedPoint point = system.getIndexService().getEncryptedPoint(id);
                assertEquals(expectedVersion, point.getVersion(), "Point " + id + " version mismatch for dim=" + dim);

                assertTrue(KeyUtils.tryDecryptWithKeyOnly(point, compromisedKey).isEmpty(),
                        "Old key should not decrypt point " + id);

                SecretKey currentKey = keyService.getVersion(point.getVersion()).getKey();
                assertTrue(KeyUtils.tryDecryptWithKeyOnly(point, currentKey).isPresent(),
                        "New key should decrypt point " + id);
            }

            long queryStart = System.nanoTime();
            List<QueryResult> results = system.query(points.get(0), TOP_K, dim);
            long queryDuration = (System.nanoTime() - queryStart) / 1_000_000;
            queryLatencies.add((double) queryDuration);

            assertNotNull(results);
            assertFalse(results.isEmpty());
            assertTrue(queryDuration < 500, "Query time must be < 500ms");
            PerformanceVisualizer.visualizeQueryResults(results);

            keyService.rotateKey();
            ((SecureLSHIndexService) system.getIndexService()).clearCache();

            for (String id : ids) {
                EncryptedPoint point = system.getIndexService().getEncryptedPoint(id);
                assertEquals(keyService.getCurrentVersion().getVersion(), point.getVersion(),
                        "Point " + id + " not updated after second rotation");
                assertTrue(KeyUtils.tryDecryptWithKeyOnly(point, compromisedKey).isEmpty(),
                        "Old key should not decrypt after second rotation");
            }

            system.shutdown();
            metadataManager.close();
        }

        PerformanceVisualizer.visualizeQueryLatencies(queryLatencies, null);
    }

    @Test
    public void testCacheHitUnderAdversarialConditions(@TempDir Path tempDir) throws Exception {
        final int dim = 128;
        final int vectorCount = 100;

        // Write config
        Path config = tempDir.resolve("config.json");
        Files.writeString(config, """
        {
          "numShards": 32,
          "profilerEnabled": true,
          "opsThreshold": 999999,
          "ageThresholdMs": 999999
        }
        """);

        // Write dummy data
        Path dummyData = tempDir.resolve("dummy.csv");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < vectorCount; i++) {
            for (int j = 0; j < dim; j++) {
                sb.append("0.1");
                if (j < dim - 1) sb.append(",");
            }
            sb.append("\n");
        }
        Files.writeString(dummyData, sb.toString());

        Path keys = tempDir.resolve("keys.ser");

        // Setup services
        KeyManager keyManager = new KeyManager(keys.toString());
        KeyRotationPolicy policy = new KeyRotationPolicy(999999, 999999);
        KeyRotationServiceImpl keyService = new KeyRotationServiceImpl(keyManager, policy, tempDir.toString(), metadataManager, null);
        CryptoService cryptoService = new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadataManager);
        keyService.setCryptoService(cryptoService);

        ForwardSecureANNSystem system = new ForwardSecureANNSystem(
                config.toString(), dummyData.toString(), keys.toString(),
                List.of(dim), tempDir, true, metadataManager, cryptoService, 1
        );
        keyService.setIndexService(system.getIndexService());

        // Insert and flush
        Random rand = new Random();
        List<String> ids = new ArrayList<>();
        List<double[]> rawVectors = new ArrayList<>();
        for (int i = 0; i < vectorCount; i++) {
            String id = UUID.randomUUID().toString();
            double[] vec = rand.doubles(dim).toArray();
            ids.add(id);
            rawVectors.add(vec);
            system.insert(id, vec, dim);
        }
        system.flushAll();

        // First query to populate cache
        List<QueryResult> results1 = system.query(rawVectors.get(0), TOP_K, dim);
        assertNotNull(results1, "Initial query results must not be null");
        assertFalse(results1.isEmpty(), "Initial query results must not be empty");

        // Perform key rotation and clear cache
        keyService.rotateKey();
        ((SecureLSHIndexService) system.getIndexService()).clearCache();

        // Second query should retrieve updated results but match structure
        List<QueryResult> results2 = system.query(rawVectors.get(0), TOP_K, dim);
        assertNotNull(results2, "Second query results must not be null");
        assertFalse(results2.isEmpty(), "Second query results must not be empty");
        assertEquals(results1.size(), results2.size(), "Query result size mismatch after re-query");

        for (int i = 0; i < results1.size(); i++) {
            QueryResult r1 = results1.get(i);
            QueryResult r2 = results2.get(i);
            assertEquals(r1.getId(), r2.getId(), "Mismatched result at index " + i);
        }

        PerformanceVisualizer.visualizeQueryResults(results2);
    }
}
