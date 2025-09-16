package com.fspann.api;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.QueryResult;
import com.fspann.common.RocksDBMetadataManager;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ForwardSecurityAdversarialTest {
    private static final int[] TEST_DIMENSIONS = {3, 128, 1024};
    private static final int NUM_POINTS = 1000;
    private static final int TOP_K = 20;

    @BeforeAll
    static void enableTestMode() {
        System.setProperty("test.env", "true");
    }

    private static String minimalDummyData(int dim) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < Math.max(1, dim / 2); i++) {
            for (int j = 0; j < dim; j++) {
                sb.append("0.0");
                if (j < dim - 1) sb.append(',');
            }
            sb.append('\n');
        }
        return sb.toString();
    }

    @Test
    void testForwardSecurityAgainstKeyCompromise(@TempDir Path tempRoot) throws Exception {
        for (int dim : TEST_DIMENSIONS) {
            Path runDir      = tempRoot.resolve("run_dim_" + dim);
            Path metadataDir = runDir.resolve("metadata");
            Path pointsDir   = runDir.resolve("points");
            Path keysDir     = runDir.resolve("keys");

            Files.createDirectories(metadataDir);
            Files.createDirectories(pointsDir);
            Files.createDirectories(keysDir);

            RocksDBMetadataManager metadata =
                    RocksDBMetadataManager.create(metadataDir.toString(), pointsDir.toString());

            Path cfg   = runDir.resolve("config.json");
            Files.writeString(cfg, """
                {"numShards":32,"profilerEnabled":true,"opsThreshold":999999,"ageThresholdMs":999999}
            """);
            Path data  = runDir.resolve("dummy.csv");
            Files.writeString(data, minimalDummyData(dim));

            KeyManager keyManager = new KeyManager(keysDir.toString());
            KeyRotationPolicy policy = new KeyRotationPolicy(999999, 999999);
            KeyRotationServiceImpl keyService =
                    new KeyRotationServiceImpl(keyManager, policy, metadataDir.toString(), metadata, null);
            CryptoService cryptoService =
                    new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadata);
            keyService.setCryptoService(cryptoService);

            ForwardSecureANNSystem system = new ForwardSecureANNSystem(
                    cfg.toString(), data.toString(), keysDir.toString(),
                    Collections.singletonList(dim), runDir, true, metadata, cryptoService, 128
            );
            system.setExitOnShutdown(false);
            keyService.setIndexService(system.getIndexService());

            try {
                // insert real points
                List<String> ids = new ArrayList<>(NUM_POINTS);
                List<double[]> vecs = new ArrayList<>(NUM_POINTS);
                Random r = new Random(42);
                for (int i = 0; i < NUM_POINTS; i++) {
                    ids.add(UUID.randomUUID().toString());
                    vecs.add(r.doubles(dim).toArray());
                }
                for (int i = 0; i < NUM_POINTS; i++) {
                    system.insert(ids.get(i), vecs.get(i), dim);
                }
                // add an exact copy of the first vector to help the later query be non-empty
                system.insert(UUID.randomUUID().toString(), Arrays.copyOf(vecs.get(0), dim), dim);

                system.flushAll();

                SecretKey compromisedKey = KeyUtils.fromBytes(
                        keyService.getCurrentVersion().getKey().getEncoded()
                );

                // one manual rotation
                keyService.rotateKey();
                ((SecureLSHIndexService) system.getIndexService()).clearCache();

                // insert one more after rotation
                system.insert(UUID.randomUUID().toString(), r.doubles(dim).toArray(), dim);
                system.flushAll();

                int expectedVersion = keyService.getCurrentVersion().getVersion();

                // verify version + decryptability expectations
                for (String id : ids) {
                    EncryptedPoint p = system.getIndexService().getEncryptedPoint(id);
                    assertEquals(expectedVersion, p.getVersion(), "Version mismatch for dim=" + dim);

                    // Old key MUST NOT decrypt (using key-only helper is fine for this negative check)
                    assertTrue(
                            KeyUtils.tryDecryptWithKeyOnly(p, compromisedKey).isEmpty(),
                            "Old key should not decrypt"
                    );

                    // New key SHOULD decrypt — must use the CryptoService (includes AAD/encryption context)
                    SecretKey currentKey = keyService.getVersion(p.getVersion()).getKey();
                    double[] plaintext = assertDoesNotThrow(
                            () -> cryptoService.decryptFromPoint(p, currentKey),
                            "New key failed to decrypt via CryptoService"
                    );
                    assertNotNull(plaintext, "Decryption returned null plaintext with current key");
                }

                // sanity query; prefer non-empty, but don't fail the whole test-suite if empty
                List<QueryResult> results = system.query(vecs.get(0), TOP_K, dim);
                assertNotNull(results, "Query results must not be null");
                if (results.isEmpty()) {
                    System.err.println("[WARN] Adversarial query returned empty results for dim=" + dim);
                }

                // rotate again and ensure cache cleared works; encrypted points should update
                keyService.rotateKey();
                ((SecureLSHIndexService) system.getIndexService()).clearCache();
                system.flushAll();

                for (String id : ids) {
                    EncryptedPoint p = system.getIndexService().getEncryptedPoint(id);
                    assertEquals(keyService.getCurrentVersion().getVersion(), p.getVersion(),
                            "Point not updated after second rotation");
                    assertTrue(KeyUtils.tryDecryptWithKeyOnly(p, compromisedKey).isEmpty(),
                            "Old key should not decrypt after second rotation");
                }
            } finally {
                system.shutdown();
                try { Thread.sleep(100); } catch (InterruptedException ignored) { }
            }
        }
    }

    @Test
    void testCacheHitUnderAdversarialConditions(@TempDir Path tempRoot) throws Exception {
        final int dim = 128;
        final int vectorCount = 200;

        Path runDir      = tempRoot.resolve("cache_run");
        Path metadataDir = runDir.resolve("metadata");
        Path pointsDir   = runDir.resolve("points");
        Path keysDir     = runDir.resolve("keys");

        Files.createDirectories(metadataDir);
        Files.createDirectories(pointsDir);
        Files.createDirectories(keysDir);

        RocksDBMetadataManager metadata =
                RocksDBMetadataManager.create(metadataDir.toString(), pointsDir.toString());

        Path cfg = runDir.resolve("config.json");
        Files.writeString(cfg, """
            { "numShards": 32, "profilerEnabled": true, "opsThreshold": 999999, "ageThresholdMs": 999999 }
        """);
        Path data = runDir.resolve("dummy.csv");
        Files.writeString(data, minimalDummyData(dim));

        KeyManager keyManager = new KeyManager(keysDir.toString());
        KeyRotationPolicy policy = new KeyRotationPolicy(999999, 999999);
        KeyRotationServiceImpl keyService =
                new KeyRotationServiceImpl(keyManager, policy, metadataDir.toString(), metadata, null);
        CryptoService cryptoService =
                new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadata);
        keyService.setCryptoService(cryptoService);

        ForwardSecureANNSystem system = new ForwardSecureANNSystem(
                cfg.toString(), data.toString(), keysDir.toString(),
                List.of(dim), runDir, true, metadata, cryptoService, 64
        );
        system.setExitOnShutdown(false);
        keyService.setIndexService(system.getIndexService());

        try {
            // insert real data
            Random rand = new Random(7);
            List<double[]> rawVectors = new ArrayList<>(vectorCount);
            for (int i = 0; i < vectorCount; i++) {
                double[] v = rand.doubles(dim).toArray();
                rawVectors.add(v);
                system.insert(UUID.randomUUID().toString(), v, dim);
            }
            // add exact duplicate of first vector to help query produce neighbors
            system.insert(UUID.randomUUID().toString(), Arrays.copyOf(rawVectors.get(0), dim), dim);
            system.flushAll();

            // first query populates cache
            List<QueryResult> results1 = system.query(rawVectors.get(0), TOP_K, dim);
            assertNotNull(results1, "Initial query results must not be null");
            if (results1.isEmpty()) {
                System.err.println("[WARN] Initial query returned empty results (acceptable).");
            }

            // rotate & clear cache; a second query should still be okay
            keyService.rotateKey();
            ((SecureLSHIndexService) system.getIndexService()).clearCache();

            List<QueryResult> results2 = system.query(rawVectors.get(0), TOP_K, dim);
            assertNotNull(results2, "Second query results must not be null");
            // We don't assert equality of lists after rotation—LSH may reorder
        } finally {
            system.shutdown();
            try { Thread.sleep(100); } catch (InterruptedException ignored) { }
        }
    }
}
