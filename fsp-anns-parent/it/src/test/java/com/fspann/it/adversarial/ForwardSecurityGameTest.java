package com.fspann.it.adversarial;

import com.fspann.api.ForwardSecureANNSystem;
import com.fspann.common.EncryptedPoint;
import com.fspann.common.RocksDBMetadataManager;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.key.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import javax.crypto.SecretKey;
import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Forward Security Game-Based Formal Test
 *
 * Security notion:
 *   Forward Secrecy under Adaptive Key Compromise (FS-AKC)
 *
 * Adversary model:
 *   - Learns all previous secret keys
 *   - Observes ciphertexts before and after re-encryption
 *   - Attempts to decrypt re-encrypted ciphertexts
 *
 * Winning condition:
 *   Any successful decryption with a compromised key
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

        Path cfg = temp.resolve("cfg.json");
        Files.writeString(cfg, """
        {
          "paper": { "enabled": true, "divisions": 3, "m": 6, "seed": 13 },
          "reencryption": { "enabled": true },
          "output": { "exportArtifacts": false }
        }
        """);

        system = new ForwardSecureANNSystem(
                cfg.toString(),
                temp.resolve("seed.csv").toString(),
                keyDir.toString(),
                List.of(3),
                temp,
                false,
                metadata,
                crypto,
                64
        );

        system.insert("p1", new double[]{1.0, 2.0, 3.0}, 3);
        system.insert("p2", new double[]{2.0, 3.0, 4.0}, 3);

        system.finalizeForSearch();

        compromisedKey = keyService.getCurrentVersion().getKey();
        compromisedVersion = keyService.getCurrentVersion().getVersion();
    }

    /**
     * Game G₀ vs G₁:
     * Adversary attempts to decrypt after key rotation
     */
    @Test
    void forwardSecrecyGame_oldKeysCannotDecrypt() {

        keyService.rotateKeyOnly();
        keyService.reEncryptAll();

        int wins = 0;

        for (EncryptedPoint p : metadata.getAllEncryptedPoints()) {
            try {
                crypto.decryptFromPoint(p, compromisedKey);
                wins++;
            } catch (Exception ignored) {}
        }

        assertEquals(
                0,
                wins,
                "Forward secrecy violated: old key decrypted re-encrypted ciphertext"
        );
    }

    /**
     * Ciphertext indistinguishability check
     */
    @Test
    void gameCiphertextIndistinguishability() {

        Map<String, byte[]> before = snapshot();

        keyService.rotateKeyOnly();
        keyService.reEncryptAll();

        Map<String, byte[]> after = snapshot();

        assertNotEquals(
                before,
                after,
                "Ciphertexts must change after re-encryption"
        );
    }

    /**
     * Selective re-encryption game
     */
    @Test
    void selectiveReencryptionGame() {

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
            assertArrayEquals(
                    before.get(ids.get(i)),
                    after.get(ids.get(i)),
                    "Untouched ciphertext modified"
            );
        }
    }

    /**
     * NEW TEST: Key tracking validation in security game
     */
    @Test
    void gameKeyTrackingIntegrity() {
        KeyManager km = keyService.getKeyManager();
        KeyUsageTracker tracker = km.getUsageTracker();

        // Initial state: 2 vectors with v1
        assertEquals(2, tracker.getVectorCount(compromisedVersion));
        assertFalse(tracker.isSafeToDelete(compromisedVersion));

        // Rotate and re-encrypt
        keyService.rotateKeyOnly();
        int newVersion = keyService.getCurrentVersion().getVersion();
        keyService.reEncryptAll();

        // After re-encryption: old version should have 0 vectors
        assertEquals(0, tracker.getVectorCount(compromisedVersion));
        assertTrue(tracker.isSafeToDelete(compromisedVersion));

        // New version should have 2 vectors
        assertEquals(2, tracker.getVectorCount(newVersion));
        assertFalse(tracker.isSafeToDelete(newVersion));
    }

    /**
     * NEW TEST: Safe deletion after rotation
     */
    @Test
    void gameSafeDeletionAfterReencryption() {
        KeyManager km = keyService.getKeyManager();

        // Rotate and re-encrypt
        keyService.rotateKeyOnly();
        keyService.reEncryptAll();

        // Try to delete old key (should succeed)
        km.deleteKeysOlderThan(compromisedVersion + 1);

        // Verify old key is deleted
        assertNull(km.getSessionKey(compromisedVersion));

        // Verify new key still exists
        assertNotNull(km.getSessionKey(compromisedVersion + 1));
    }

    private Map<String, byte[]> snapshot() {
        Map<String, byte[]> m = new HashMap<>();
        for (EncryptedPoint p : metadata.getAllEncryptedPoints()) {
            m.put(p.getId(), Arrays.copyOf(
                    p.getCiphertext(),
                    p.getCiphertext().length
            ));
        }
        return m;
    }

    @AfterEach
    void cleanup() {
        try { system.setExitOnShutdown(false); system.shutdown(); } catch (Exception ignore) {}
        try { metadata.close(); } catch (Exception ignore) {}
    }
}