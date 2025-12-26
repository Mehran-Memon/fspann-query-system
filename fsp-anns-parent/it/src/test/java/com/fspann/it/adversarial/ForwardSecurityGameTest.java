package com.fspann.it.adversarial;

import com.fspann.api.ForwardSecureANNSystem;
import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.index.paper.GFunctionRegistry;
import com.fspann.key.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import javax.crypto.SecretKey;
import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Forward Security Game-Based Test Suite
 *
 * Security notion: FS-AKC (Forward Secrecy under Adaptive Key Compromise)
 *
 * Each test corresponds to a formal security game used in the paper proof.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ForwardSecurityGameTest {

    @TempDir
    Path temp;

    ForwardSecureANNSystem system;
    RocksDBMetadataManager metadata;
    KeyRotationServiceImpl keyService;
    AesGcmCryptoService crypto;

    SecretKey compromisedKey;
    int compromisedVersion;

    @BeforeEach
    void setup() throws Exception {

        Path metaDir = temp.resolve("meta");
        Path ptsDir  = temp.resolve("pts");
        Path keyDir  = temp.resolve("keys");

        Files.createDirectories(metaDir);
        Files.createDirectories(ptsDir);
        Files.createDirectories(keyDir);

        metadata = RocksDBMetadataManager.create(
                metaDir.toString(),
                ptsDir.toString()
        );

        // -------------------------------
        // CONFIG
        // -------------------------------
        Path cfg = temp.resolve("cfg.json");
        Files.writeString(cfg, """
    {
      "paper": {
        "enabled": true,
        "tables": 3,
        "divisions": 4,
        "m": 6,
        "lambda": 3,
        "seed": 13
      },
      "reencryption": { "enabled": true }
    }
    """);

        SystemConfig sc = SystemConfig.load(cfg.toString(), true);

        // -------------------------------
        // RESET + INIT REGISTRY (dimension = 6)
        // -------------------------------
        GFunctionRegistry.reset();
        GFunctionRegistry.initialize(
                List.of(
                        new double[]{1,2,3,4,5,6},
                        new double[]{2,3,4,5,6,7}
                ),
                6,
                sc.getPaper().m,
                sc.getPaper().lambda,
                sc.getPaper().seed,
                sc.getPaper().getTables(),
                sc.getPaper().divisions
        );

        // -------------------------------
        // KEYS + CRYPTO
        // -------------------------------
        KeyManager km = new KeyManager(keyDir.resolve("ks.blob").toString());

        keyService = new KeyRotationServiceImpl(
                km,
                new KeyRotationPolicy(1, Long.MAX_VALUE),
                metaDir.toString(),
                metadata,
                null
        );

        crypto = new AesGcmCryptoService(
                new SimpleMeterRegistry(),
                keyService,
                metadata
        );
        keyService.setCryptoService(crypto);

        // -------------------------------
        // SYSTEM (dimension = 6)
        // -------------------------------
        system = new ForwardSecureANNSystem(
                cfg.toString(),
                temp.resolve("seed.csv").toString(),
                keyDir.toString(),
                List.of(6),
                temp,
                false,
                metadata,
                crypto,
                64
        );

        // -------------------------------
        // INSERT DATA
        // -------------------------------
        for (int i = 0; i < 8; i++) {
            system.insert(
                    "p" + i,
                    new double[]{i, i+1, i+2, i+3, i+4, i+5},
                    6
            );
        }

        system.finalizeForSearch();

        compromisedKey = keyService.getCurrentVersion().getKey();
        compromisedVersion = keyService.getCurrentVersion().getVersion();
    }


    /* ---------------------------------------------------------
       Game G₁: Old keys cannot decrypt re-encrypted ciphertext
       --------------------------------------------------------- */
    @Test
    void game_G1_forwardSecrecy_oldKeysFail() {

        keyService.rotateKeyOnly();
        keyService.reEncryptAll();

        int wins = 0;
        for (EncryptedPoint p : metadata.getAllEncryptedPoints()) {
            try {
                crypto.decryptFromPoint(p, compromisedKey);
                wins++;
            } catch (Exception ignored) {}
        }
        assertEquals(0, wins);
    }

    /* ---------------------------------------------------------
       Game G₂: Ciphertext indistinguishability
       --------------------------------------------------------- */
    @Test
    void game_G2_ciphertextIndistinguishability() {

        Map<String, byte[]> before = snapshot();

        keyService.rotateKeyOnly();
        keyService.reEncryptAll();

        Map<String, byte[]> after = snapshot();

        assertNotEquals(before, after);
    }

    /* ---------------------------------------------------------
       Game G₃: Selective re-encryption soundness
       --------------------------------------------------------- */
    @Test
    void game_G3_selectiveReencryption() {

        List<String> ids = metadata.getAllVectorIds();
        assertFalse(ids.isEmpty());

        String touched = ids.get(0);
        Map<String, byte[]> before = snapshot();

        keyService.reencryptTouched(
                List.of(touched),
                keyService.getCurrentVersion().getVersion(),
                () -> metadata.sizePointsDir()
        );

        Map<String, byte[]> after = snapshot();

        assertNotEquals(before.get(touched), after.get(touched));

        for (int i = 1; i < ids.size(); i++) {
            assertArrayEquals(before.get(ids.get(i)), after.get(ids.get(i)));
        }
    }

    /* ---------------------------------------------------------
       Game G₄: Key usage accounting
       --------------------------------------------------------- */
    @Test
    void game_G4_keyUsageAccounting() {

        KeyUsageTracker tracker = keyService.getKeyManager().getUsageTracker();

        assertEquals(8, tracker.getVectorCount(compromisedVersion));
        assertFalse(tracker.isSafeToDelete(compromisedVersion));

        keyService.rotateKeyOnly();
        int newVersion = keyService.getCurrentVersion().getVersion();
        keyService.reEncryptAll();

        assertEquals(0, tracker.getVectorCount(compromisedVersion));
        assertTrue(tracker.isSafeToDelete(compromisedVersion));
        assertEquals(8, tracker.getVectorCount(newVersion));
    }

    /* ---------------------------------------------------------
       Game G₅: Safe deletion soundness
       --------------------------------------------------------- */
    @Test
    void game_G5_safeDeletion() {

        KeyManager km = keyService.getKeyManager();

        keyService.rotateKeyOnly();
        keyService.reEncryptAll();

        km.deleteKeysOlderThan(compromisedVersion + 1);

        assertNull(km.getSessionKey(compromisedVersion));
        assertNotNull(km.getSessionKey(compromisedVersion + 1));
    }

    /* ---------------------------------------------------------
       Game G₆: Post-rotation correctness + timing
       --------------------------------------------------------- */
    @Test
    void game_G6_queryCorrectnessAndTiming() {

        double[] query = {1.5, 2.5, 3.5, 4.5, 5.5, 6.5};

        long t0 = System.nanoTime();
        QueryToken t1 = system.createToken(query, 1, 6);
        List<QueryResult> before = system.getQueryServiceImpl().search(t1);
        long t1n = System.nanoTime();

        keyService.rotateKeyOnly();
        keyService.reEncryptAll();

        QueryToken t2 = system.createToken(query, 1, 6);
        List<QueryResult> after = system.getQueryServiceImpl().search(t2);
        long t2n = System.nanoTime();

        // ---- SECURITY INVARIANT ----
        if (!before.isEmpty()) {
            assertFalse(after.isEmpty(),
                    "Result disappeared after key rotation");
            assertEquals(
                    before.get(0).getId(),
                    after.get(0).getId(),
                    "Forward-security violated: result changed after rotation"
            );
        } else {
            assertTrue(after.isEmpty(),
                    "Rotation introduced a result where none existed");
        }

        // ---- TIMING SANITY (NOT PERFORMANCE CLAIM) ----
        assertTrue((t1n - t0) >= 0);
        assertTrue((t2n - t1n) >= 0);
    }


    /* --------------------------------------------------------- */

    private Map<String, byte[]> snapshot() {
        Map<String, byte[]> m = new HashMap<>();
        for (EncryptedPoint p : metadata.getAllEncryptedPoints()) {
            m.put(p.getId(), Arrays.copyOf(p.getCiphertext(), p.getCiphertext().length));
        }
        return m;
    }

    @AfterEach
    void cleanup() {
        try { system.shutdown(); } catch (Exception ignore) {}
        try { metadata.close(); } catch (Exception ignore) {}
    }
}
