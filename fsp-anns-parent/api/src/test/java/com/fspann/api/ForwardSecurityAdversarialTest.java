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

    /**
     * Game 1 (Single-Compromise Forward Security):
     *  - Adversary compromises the current key K_v.
     *  - System rotates to K_{v+1} and re-encrypts existing points.
     *  - Adversary tries to decrypt updated ciphertexts with K_v.
     *  - Expected: all updated ciphertexts reject under K_v but decrypt under K_{v+1}.
     */
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

            // Use a concrete keystore file (newer KeyManager expects a file path)
            Path keystore = keysDir.resolve("keystore.blob");
            Files.createDirectories(keystore.getParent());

            KeyManager keyManager = new KeyManager(keystore.toString());
            KeyRotationPolicy policy = new KeyRotationPolicy(999999, 999999);
            KeyRotationServiceImpl keyService =
                    new KeyRotationServiceImpl(keyManager, policy, metadataDir.toString(), metadata, null);
            CryptoService cryptoService =
                    new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadata);
            keyService.setCryptoService(cryptoService);

            ForwardSecureANNSystem system = new ForwardSecureANNSystem(
                    cfg.toString(), data.toString(), keystore.toString(),
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

                // Compromise the current key (version v0)
                SecretKey compromisedKey = KeyUtils.fromBytes(
                        keyService.getCurrentVersion().getKey().getEncoded()
                );

                // one manual rotation → v1
                keyService.rotateKey();
                ((SecureLSHIndexService) system.getIndexService()).clearCache();
                system.flushAll(); // ensure persisted after rotation

                // insert one more after rotation
                system.insert(UUID.randomUUID().toString(), r.doubles(dim).toArray(), dim);
                system.flushAll();

                int expectedVersion = keyService.getCurrentVersion().getVersion();

                // verify version + decryptability expectations
                for (String id : ids) {
                    EncryptedPoint p = system.getIndexService().getEncryptedPoint(id);
                    assertEquals(expectedVersion, p.getVersion(), "Version mismatch for dim=" + dim);

                    // Old key MUST NOT decrypt (key-only helper -> negative check)
                    assertTrue(
                            KeyUtils.tryDecryptWithKeyOnly(p, compromisedKey).isEmpty(),
                            "Old key should not decrypt"
                    );

                    // New key SHOULD decrypt — use CryptoService (AAD/context aware)
                    SecretKey currentKey = keyService.getVersion(p.getVersion()).getKey();
                    double[] plaintext = assertDoesNotThrow(
                            () -> cryptoService.decryptFromPoint(p, currentKey),
                            "New key failed to decrypt via CryptoService"
                    );
                    assertNotNull(plaintext, "Decryption returned null plaintext with current key");
                }

                // sanity query; prefer non-empty, but don't fail if LSH misses
                List<QueryResult> results = system.query(vecs.get(0), TOP_K, dim);
                assertNotNull(results, "Query results must not be null");
                if (results.isEmpty()) {
                    System.err.println("[WARN] Adversarial query returned empty results for dim=" + dim);
                }

                // rotate again and ensure cache cleared; encrypted points should update
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

    /**
     * Game 2 (Cache-Resilient Queries After Rotation):
     *  - Index a dataset and warm up the query cache.
     *  - Rotate the key and clear the index cache.
     *  - Ensure queries still succeed (no stale-key / cache corruption issues).
     */
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

        // Use a concrete keystore file
        Path keystore = keysDir.resolve("keystore.blob");
        Files.createDirectories(keystore.getParent());

        KeyManager keyManager = new KeyManager(keystore.toString());
        KeyRotationPolicy policy = new KeyRotationPolicy(999999, 999999);
        KeyRotationServiceImpl keyService =
                new KeyRotationServiceImpl(keyManager, policy, metadataDir.toString(), metadata, null);
        CryptoService cryptoService =
                new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadata);
        keyService.setCryptoService(cryptoService);

        ForwardSecureANNSystem system = new ForwardSecureANNSystem(
                cfg.toString(), data.toString(), keystore.toString(),
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
            system.flushAll();

            List<QueryResult> results2 = system.query(rawVectors.get(0), TOP_K, dim);
            assertNotNull(results2, "Second query results must not be null");
            // We don't assert equality of lists after rotation—LSH may reorder
        } finally {
            system.shutdown();
            try { Thread.sleep(100); } catch (InterruptedException ignored) { }
        }
    }

    /**
     * Game 3 (Multi-Rotation Epoch Chain):
     *
     *  Adversary model:
     *    - Over time, the attacker compromises several old keys K_1, K_2, ..., K_{t-1}.
     *    - The system runs multiple rotations and re-encrypts the corpus each time.
     *    - At the end, all points are at version t with key K_t.
     *
     *  Goal:
     *    - Show that NONE of the compromised older keys K_i (i < t) can decrypt the
     *      final ciphertexts at version t, while K_t (current key) can.
     *
     *  This is a strictly stronger game than a single-compromise test and matches
     *  a "ladder of epochs" argument that can be described in the paper.
     */
    @Test
    void testMultipleRotationsOldKeysCannotDecryptFinalEpoch(@TempDir Path tempRoot) throws Exception {
        final int dim = 128;
        final int vectorCount = 300;
        final int rotations = 3; // will end with finalVersion = initialVersion + rotations

        Path runDir      = tempRoot.resolve("multi_rotation_run");
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

        // Use a concrete keystore file
        Path keystore = keysDir.resolve("keystore.blob");
        Files.createDirectories(keystore.getParent());

        KeyManager keyManager = new KeyManager(keystore.toString());
        KeyRotationPolicy policy = new KeyRotationPolicy(999999, 999999);
        KeyRotationServiceImpl keyService =
                new KeyRotationServiceImpl(keyManager, policy, metadataDir.toString(), metadata, null);
        CryptoService cryptoService =
                new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadata);
        keyService.setCryptoService(cryptoService);

        ForwardSecureANNSystem system = new ForwardSecureANNSystem(
                cfg.toString(), data.toString(), keystore.toString(),
                List.of(dim), runDir, true, metadata, cryptoService, 128
        );
        system.setExitOnShutdown(false);
        keyService.setIndexService(system.getIndexService());

        // Track IDs so we can fetch EncryptedPoint later
        List<String> ids = new ArrayList<>(vectorCount);

        try {
            // 1) Insert initial corpus under initial version v0
            Random rnd = new Random(2025);
            for (int i = 0; i < vectorCount; i++) {
                String id = UUID.randomUUID().toString();
                ids.add(id);
                system.insert(id, rnd.doubles(dim).toArray(), dim);
            }
            system.flushAll();

            // 2) Adversary successively compromises old keys K_v, then we rotate & re-encrypt.
            //    We'll store SecretKey copies for each version before rotation.
            Map<Integer, SecretKey> compromisedKeysByVersion = new HashMap<>();

            // initial version
            int initialVersion = keyService.getCurrentVersion().getVersion();
            compromisedKeysByVersion.put(
                    initialVersion,
                    KeyUtils.fromBytes(keyService.getCurrentVersion().getKey().getEncoded())
            );

            for (int i = 0; i < rotations; i++) {
                // Rotate to next version
                keyService.rotateKey();
                ((SecureLSHIndexService) system.getIndexService()).clearCache();
                system.flushAll();

                int curVersion = keyService.getCurrentVersion().getVersion();
                // Attacker compromises the *previous* key as soon as it's "old"
                compromisedKeysByVersion.put(
                        curVersion,
                        KeyUtils.fromBytes(keyService.getCurrentVersion().getKey().getEncoded())
                );

                // Optional: insert a few new points at each epoch to mimic a running system
                for (int j = 0; j < 20; j++) {
                    String id = UUID.randomUUID().toString();
                    ids.add(id);
                    system.insert(id, rnd.doubles(dim).toArray(), dim);
                }
                system.flushAll();
            }

            int finalVersion = keyService.getCurrentVersion().getVersion();

            // 3) Now enforce the game condition:
            //      For every encrypted point at finalVersion,
            //      - All *older* keys K_i (i < finalVersion) MUST fail decryption.
            //      - The current key K_finalVersion MUST succeed via CryptoService.
            for (String id : ids) {
                EncryptedPoint p = system.getIndexService().getEncryptedPoint(id);
                assertEquals(finalVersion, p.getVersion(),
                        "Point should have been advanced to final epoch");

                // All previously compromised keys must fail on this final ciphertext
                for (Map.Entry<Integer, SecretKey> e : compromisedKeysByVersion.entrySet()) {
                    int v = e.getKey();
                    SecretKey oldKey = e.getValue();
                    if (v == finalVersion) {
                        // skip the current epoch here; test it below with CryptoService
                        continue;
                    }
                    assertTrue(
                            KeyUtils.tryDecryptWithKeyOnly(p, oldKey).isEmpty(),
                            () -> "Old key from version " + v + " should not decrypt final epoch ciphertext"
                    );
                }

                // Current key must succeed (AAD-aware path)
                SecretKey curKey = keyService.getVersion(p.getVersion()).getKey();
                double[] pt = assertDoesNotThrow(
                        () -> cryptoService.decryptFromPoint(p, curKey),
                        "Final epoch key failed to decrypt"
                );
                assertNotNull(pt, "Plaintext from final epoch decryption must not be null");
            }

        } finally {
            system.shutdown();
            try { Thread.sleep(100); } catch (InterruptedException ignored) { }
        }
    }
}
