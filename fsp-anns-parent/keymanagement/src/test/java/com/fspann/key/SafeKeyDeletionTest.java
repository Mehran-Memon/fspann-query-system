package com.fspann.key;

import com.fspann.common.*;
import com.fspann.crypto.AesGcmCryptoService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for safe key deletion with usage tracking.
 *
 * CRITICAL: Must properly close RocksDB resources to avoid file locks.
 */
public class SafeKeyDeletionTest {

    @TempDir
    Path tempDir;

    private KeyManager keyManager;
    private RocksDBMetadataManager metadataManager;
    private AesGcmCryptoService cryptoService;
    private KeyRotationServiceImpl keyService;

    @BeforeEach
    public void setup() throws Exception {
        Path metaPath = tempDir.resolve("metadata");
        Path pointsPath = tempDir.resolve("points");

        // Create metadata manager
        metadataManager = RocksDBMetadataManager.create(
                metaPath.toString(),
                pointsPath.toString()
        );

        // Create key manager
        keyManager = new KeyManager(tempDir.resolve("keystore.blob").toString());

        // Create key rotation service
        keyService = new KeyRotationServiceImpl(
                keyManager,
                new KeyRotationPolicy(Integer.MAX_VALUE, Long.MAX_VALUE),
                metaPath.toString(),
                metadataManager,
                null
        );

        // Create crypto service
        cryptoService = new AesGcmCryptoService(
                new SimpleMeterRegistry(),
                keyService,
                metadataManager
        );

        keyService.setCryptoService(cryptoService);
    }

    @AfterEach
    public void cleanup() {
        // CRITICAL: Close RocksDB to release file locks
        if (metadataManager != null) {
            try {
                metadataManager.close();
            } catch (Exception e) {
                // Log but don't fail test
                System.err.println("Failed to close metadata manager: " + e.getMessage());
            }
        }
    }

    @Test
    public void testCannotDeleteKeyWithBoundVectors() {
        // Encrypt vector with v1
        double[] vec = {1.0, 2.0, 3.0};
        EncryptedPoint ep = cryptoService.encrypt("vec1", vec);
        assertEquals(1, ep.getKeyVersion());

        // Try to delete v1 (should fail - vec1 still using it)
        keyManager.deleteKeysOlderThan(2);

        // v1 should still exist
        assertNotNull(keyManager.getSessionKey(1), "v1 should NOT be deleted");

        KeyUsageTracker tracker = keyManager.getUsageTracker();
        assertEquals(1, tracker.getVectorCount(1));
        assertFalse(tracker.isSafeToDelete(1));
    }

    @Test
    public void testCanDeleteKeyAfterReencryption() throws Exception {
        // Encrypt vector with v1
        double[] vec = {1.0, 2.0, 3.0};
        EncryptedPoint ep1 = cryptoService.encrypt("vec1", vec);
        assertEquals(1, ep1.getKeyVersion());

        // IMPORTANT: Save to metadata so reencryptTouched can find it
        metadataManager.saveEncryptedPoint(ep1);

        // Rotate to v2
        keyService.forceRotateNow();
        int v2 = keyService.getCurrentVersion().getVersion();
        assertEquals(2, v2);

        // Re-encrypt vec1 to v2
        ReencryptReport report = keyService.reencryptTouched(
                java.util.Set.of("vec1"),
                v2,
                () -> 0L
        );

        // Verify re-encryption happened
        assertTrue(report.reencryptedCount() > 0, "Should have re-encrypted at least 1 vector");

        // Now delete v1 (should succeed)
        keyManager.deleteKeysOlderThan(v2);

        // v1 should be deleted
        assertNull(keyManager.getSessionKey(1), "v1 should be deleted");

        KeyUsageTracker tracker = keyManager.getUsageTracker();
        assertEquals(0, tracker.getVectorCount(1));
        assertTrue(tracker.isSafeToDelete(1));
    }

    @Test
    public void testBatchReencryptionAndDeletion() throws Exception {
        // Encrypt 100 vectors with v1
        for (int i = 0; i < 100; i++) {
            double[] vec = {i, i + 1.0, i + 2.0};
            EncryptedPoint ep = cryptoService.encrypt("vec" + i, vec);
            metadataManager.saveEncryptedPoint(ep);
        }

        KeyUsageTracker tracker = keyManager.getUsageTracker();
        assertEquals(100, tracker.getVectorCount(1));

        // Rotate to v2
        keyService.forceRotateNow();
        int v2 = keyService.getCurrentVersion().getVersion();

        // Re-encrypt all to v2
        java.util.Set<String> allIds = new java.util.HashSet<>();
        for (int i = 0; i < 100; i++) {
            allIds.add("vec" + i);
        }

        ReencryptReport report = keyService.reencryptTouched(allIds, v2, () -> 0L);

        assertEquals(100, report.touchedCount());
        assertEquals(100, report.reencryptedCount());

        // v1 should now have 0 vectors
        assertEquals(0, tracker.getVectorCount(1));
        assertTrue(tracker.isSafeToDelete(1));

        // Delete v1
        keyManager.deleteKeysOlderThan(v2);
        assertNull(keyManager.getSessionKey(1));
    }

    @Test
    public void testPartialReencryptionPreventsDeletion() throws Exception {
        // Encrypt 10 vectors with v1
        for (int i = 0; i < 10; i++) {
            EncryptedPoint ep = cryptoService.encrypt("vec" + i, new double[]{i, i, i});
            metadataManager.saveEncryptedPoint(ep);
        }

        // Rotate to v2
        keyService.forceRotateNow();
        int v2 = keyService.getCurrentVersion().getVersion();

        // Re-encrypt only 5 vectors
        java.util.Set<String> partial = new java.util.HashSet<>();
        for (int i = 0; i < 5; i++) {
            partial.add("vec" + i);
        }

        keyService.reencryptTouched(partial, v2, () -> 0L);

        KeyUsageTracker tracker = keyManager.getUsageTracker();
        assertEquals(5, tracker.getVectorCount(1), "5 vectors still use v1");
        assertFalse(tracker.isSafeToDelete(1));

        // Try to delete v1 (should skip)
        keyManager.deleteKeysOlderThan(v2);

        // v1 should still exist
        assertNotNull(keyManager.getSessionKey(1), "v1 should NOT be deleted");
    }
}