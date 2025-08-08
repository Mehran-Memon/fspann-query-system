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
        // tiny file just to satisfy loader; you insert real points later
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
            // --- fresh dirs per dimension (no shared metadata/key state) ---
            Path runDir      = tempRoot.resolve("run_dim_" + dim);
            Path metadataDir = runDir.resolve("metadata");
            Path pointsDir   = runDir.resolve("points");
            Path keysDir     = runDir.resolve("keys");

            Files.createDirectories(metadataDir);
            Files.createDirectories(pointsDir);
            Files.createDirectories(keysDir);

            // fresh RocksDB per run
            RocksDBMetadataManager metadata = RocksDBMetadataManager.create(metadataDir.toString(), pointsDir.toString());

            // tiny config + dummy data file
            Path cfg   = runDir.resolve("config.json");
            Files.writeString(cfg, "{\"numShards\":32,\"profilerEnabled\":true,\"opsThreshold\":999999,\"ageThresholdMs\":999999}");
            Path data  = runDir.resolve("dummy.csv");
            Files.writeString(data, minimalDummyData(dim));

            // key / crypto
            KeyManager keyManager = new KeyManager(keysDir.toString());
            KeyRotationPolicy policy = new KeyRotationPolicy(999999, 999999);
            KeyRotationServiceImpl keyService = new KeyRotationServiceImpl(keyManager, policy, metadataDir.toString(), metadata, null);
            CryptoService cryptoService = new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadata);
            keyService.setCryptoService(cryptoService);

            ForwardSecureANNSystem system = new ForwardSecureANNSystem(
                    cfg.toString(), data.toString(), keysDir.toString(),
                    Collections.singletonList(dim), runDir, true, metadata, cryptoService, 64
            );
            system.setExitOnShutdown(false);
            keyService.setIndexService(system.getIndexService());

            try {
                // insert real points
                List<String> ids = new ArrayList<>(NUM_POINTS);
                List<double[]> vecs = new ArrayList<>(NUM_POINTS);
                Random r = new Random();
                for (int i = 0; i < NUM_POINTS; i++) {
                    ids.add(UUID.randomUUID().toString());
                    vecs.add(r.doubles(dim).toArray());
                }
                for (int i = 0; i < NUM_POINTS; i++) {
                    system.insert(ids.get(i), vecs.get(i), dim);
                }
                system.flushAll();

                SecretKey compromisedKey = KeyUtils.fromBytes(keyService.getCurrentVersion().getKey().getEncoded());

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

                    assertTrue(KeyUtils.tryDecryptWithKeyOnly(p, compromisedKey).isEmpty(), "Old key should not decrypt");
                    SecretKey currentKey = keyService.getVersion(p.getVersion()).getKey();
                    assertTrue(KeyUtils.tryDecryptWithKeyOnly(p, currentKey).isPresent(), "New key should decrypt");
                }

                // sanity query; should not be empty
                List<QueryResult> results = system.query(vecs.get(0), TOP_K, dim);
                assertNotNull(results);
                assertFalse(results.isEmpty(), "Query returned no results for dim=" + dim);

                // rotate again and ensure cache cleared works
                keyService.rotateKey();
                ((SecureLSHIndexService) system.getIndexService()).clearCache();

                for (String id : ids) {
                    EncryptedPoint p = system.getIndexService().getEncryptedPoint(id);
                    assertEquals(keyService.getCurrentVersion().getVersion(), p.getVersion(),
                            "Point not updated after second rotation");
                    assertTrue(KeyUtils.tryDecryptWithKeyOnly(p, compromisedKey).isEmpty(),
                            "Old key should not decrypt after second rotation");
                }
            } finally {
                // always close this run’s resources before next dim
                system.shutdown();         // closes metadata internally
                // small pause helps Windows release LOCK
                try { Thread.sleep(150); } catch (InterruptedException ignored) {}
            }
        }
    }

    @Test
    void testCacheHitUnderAdversarialConditions(@TempDir Path tempRoot) throws Exception {
        final int dim = 128;
        final int vectorCount = 100;

        Path runDir      = tempRoot.resolve("cache_run");
        Path metadataDir = runDir.resolve("metadata");
        Path pointsDir   = runDir.resolve("points");
        Path keysDir     = runDir.resolve("keys");

        Files.createDirectories(metadataDir);
        Files.createDirectories(pointsDir);
        Files.createDirectories(keysDir);

        RocksDBMetadataManager metadata = RocksDBMetadataManager.create(metadataDir.toString(), pointsDir.toString());

        Path cfg = runDir.resolve("config.json");
        Files.writeString(cfg, """
        { "numShards": 32, "profilerEnabled": true, "opsThreshold": 999999, "ageThresholdMs": 999999 }
        """);
        Path data = runDir.resolve("dummy.csv");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 5; i++) {                // tiny file; we insert real data below
            for (int j = 0; j < dim; j++) {
                sb.append("0.1");
                if (j < dim - 1) sb.append(",");
            }
            sb.append("\n");
        }
        Files.writeString(data, sb.toString());

        KeyManager keyManager = new KeyManager(keysDir.toString());
        KeyRotationPolicy policy = new KeyRotationPolicy(999999, 999999);
        KeyRotationServiceImpl keyService = new KeyRotationServiceImpl(keyManager, policy, metadataDir.toString(), metadata, null);
        CryptoService cryptoService = new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadata);
        keyService.setCryptoService(cryptoService);

        ForwardSecureANNSystem system = new ForwardSecureANNSystem(
                cfg.toString(), data.toString(), keysDir.toString(),
                List.of(dim), runDir, true, metadata, cryptoService, 1
        );
        system.setExitOnShutdown(false);
        keyService.setIndexService(system.getIndexService());

        try {
            // insert real data
            Random rand = new Random();
            List<double[]> rawVectors = new ArrayList<>(vectorCount);
            for (int i = 0; i < vectorCount; i++) {
                double[] v = rand.doubles(dim).toArray();
                rawVectors.add(v);
                system.insert(UUID.randomUUID().toString(), v, dim);
            }
            system.flushAll();

            // first query populates cache
            List<QueryResult> results1 = system.query(rawVectors.get(0), TOP_K, dim);
            assertNotNull(results1, "Initial query results must not be null");
            assertFalse(results1.isEmpty(), "Initial query results must not be empty");

            // rotate & clear cache; a second query should still return non-empty results
            keyService.rotateKey();
            ((SecureLSHIndexService) system.getIndexService()).clearCache();

            List<QueryResult> results2 = system.query(rawVectors.get(0), TOP_K, dim);
            assertNotNull(results2, "Second query results must not be null");
            assertFalse(results2.isEmpty(), "Second query results must not be empty");
            assertEquals(results1.size(), results2.size(), "Result size mismatch after re-query");

            for (int i = 0; i < results1.size(); i++) {
                assertEquals(results1.get(i).getId(), results2.get(i).getId(), "Mismatched id at " + i);
            }
        } finally {
            system.shutdown();
            try { Thread.sleep(150); } catch (InterruptedException ignored) {}
        }
    }
}
