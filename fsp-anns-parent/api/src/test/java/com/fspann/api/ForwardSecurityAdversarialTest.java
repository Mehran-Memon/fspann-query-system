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
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class ForwardSecurityAdversarialTest {
    private static RocksDBMetadataManager metadataManager;
    private static final int[] TEST_DIMENSIONS = {3, 128, 1024};
    private static final int NUM_POINTS = 1000;
    private static final int TOP_K = 20;

    @BeforeAll
    public static void setupClass(@TempDir Path tempDir) throws IOException {
        metadataManager = new RocksDBMetadataManager(tempDir.toString(), tempDir.resolve("points").toString());
    }

    @AfterAll
    public static void tearDownClass(@TempDir Path tempDir) throws IOException {
        if (metadataManager != null) {
            metadataManager.close();
        }
        for (int i = 0; i < 3; i++) {
            try {
                if (metadataManager != null) {
                    metadataManager.close();
                }
                System.gc();
                Thread.sleep(200);

                try (Stream<Path> files = Files.walk(tempDir)) {
                    files.sorted(Comparator.reverseOrder())
                            .forEach(path -> {
                                try {
                                    Files.deleteIfExists(path);
                                } catch (IOException e) {
                                    System.err.println("Failed to delete " + path);
                                }
                            });
                }
                break;
            } catch (IOException e) {
                if (i == 2) throw new IOException("Failed to delete temp directory after retries", e);
            } catch (InterruptedException ignored) {}
        }
    }

    @BeforeEach
    public void cleanMetadataDir(@TempDir Path tempDir) throws IOException {
        Path baseDir = Paths.get(metadataManager.getPointsBaseDir());
        if (Files.exists(baseDir)) {
            try (Stream<Path> files = Files.walk(baseDir)) {
                files.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(f -> {
                            if (!f.delete()) {
                                System.err.println("Failed to delete " + f.getAbsolutePath());
                            }
                        });
            }
        }
        Files.createDirectories(baseDir);
    }

    private String generateDummyData(int dim) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < NUM_POINTS; i++) {
            for (int j = 0; j < dim; j++) {
                sb.append("0.0");
                if (j < dim - 1) sb.append(",");
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    @Test
    public void testForwardSecurityAgainstKeyCompromise(@TempDir Path tempDir) throws Exception {
        List<Double> queryLatencies = new ArrayList<>();
        for (int dim : TEST_DIMENSIONS) {
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
                double[] point = new double[dim];
                for (int j = 0; j < dim; j++) {
                    point[j] = rand.nextDouble();
                }
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
            int preRotationVersion = keyService.getCurrentVersion().getVersion();

            keyService.rotateKey(); // Already re-encrypts internally
            ((SecureLSHIndexService) system.getIndexService()).clearCache();

            String afterId = UUID.randomUUID().toString();
            double[] pointAfter = new double[dim];
            for (int j = 0; j < dim; j++) {
                pointAfter[j] = rand.nextDouble();
            }
            system.insert(afterId, pointAfter, dim);
            system.flushAll();

            int expectedVersion = keyService.getCurrentVersion().getVersion();

            for (String id : ids) {
                EncryptedPoint point = system.getIndexService().getEncryptedPoint(id);
                assertEquals(expectedVersion, point.getVersion(),
                        "Point " + id + " should have updated version for dim=" + dim);

                Optional<double[]> decryptedOld = KeyUtils.tryDecryptWithKeyOnly(point, compromisedKey);
                assertTrue(decryptedOld.isEmpty(), "Old key must NOT decrypt re-encrypted point " + id + " for dim=" + dim);

                SecretKey currentKey = keyService.getVersion(point.getVersion()).getKey();
                Optional<double[]> decryptedNew = KeyUtils.tryDecryptWithKeyOnly(point, currentKey);
                assertTrue(decryptedNew.isPresent(), "New key should decrypt point " + id + " for dim=" + dim);
            }

            long queryStart = System.nanoTime();
            List<QueryResult> results = system.query(points.get(0), TOP_K, dim);
            long queryDuration = (System.nanoTime() - queryStart) / 1_000_000;
            queryLatencies.add((double) queryDuration);
            assertNotNull(results, "Query results should not be null for dim=" + dim);
            assertFalse(results.isEmpty(), "Query results should not be empty for dim=" + dim);
            assertTrue(queryDuration < 500, "Query time should be under 500ms for dim=" + dim);
            PerformanceVisualizer.visualizeQueryResults(results);

            keyService.rotateKey(); // Second rotation with internal re-encryption
            ((SecureLSHIndexService) system.getIndexService()).clearCache();

            for (String id : ids) {
                EncryptedPoint point = system.getIndexService().getEncryptedPoint(id);
                int newExpectedVersion = keyService.getCurrentVersion().getVersion();
                assertEquals(newExpectedVersion, point.getVersion(),
                        "Point " + id + " should have updated version after second rotation for dim=" + dim);
                Optional<double[]> decryptedOld = KeyUtils.tryDecryptWithKeyOnly(point, compromisedKey);
                assertTrue(decryptedOld.isEmpty(), "Old key must NOT decrypt after second rotation for dim=" + dim);
            }
        }
        PerformanceVisualizer.visualizeQueryLatencies(queryLatencies, null);
    }

    @Test
    public void testCacheHitUnderAdversarialConditions(@TempDir Path tempDir) throws Exception {
        // This method is unchanged
    }
}
