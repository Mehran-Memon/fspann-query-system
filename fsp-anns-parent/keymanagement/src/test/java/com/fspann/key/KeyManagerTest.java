package com.fspann.key;

import com.fspann.common.KeyVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

@DisplayName("KeyManager Unit Tests")
public class KeyManagerTest {

    private KeyManager keyManager;
    private Path keyStorePath;

    @TempDir
    Path tempDir;

    @BeforeEach
    public void setUp() throws IOException {
        keyStorePath = tempDir.resolve("keystore.blob");
        keyManager = new KeyManager(keyStorePath.toString());
    }

    @Test
    @DisplayName("Test KeyManager initializes with new keystore")
    public void testInitializeNewKeystore() throws IOException {
        KeyVersion current = keyManager.getCurrentVersion();
        assertNotNull(current);
        assertEquals(1, current.getVersion());
        assertNotNull(current.getKey());
    }

    @Test
    @DisplayName("Test KeyManager returns current version")
    public void testGetCurrentVersion() {
        KeyVersion current = keyManager.getCurrentVersion();
        assertNotNull(current);
        assertEquals(1, current.getVersion());
    }

    @Test
    @DisplayName("Test KeyManager rotates key")
    public void testRotateKey() {
        KeyVersion v1 = keyManager.getCurrentVersion();
        assertEquals(1, v1.getVersion());

        KeyVersion v2 = keyManager.rotateKey();
        assertEquals(2, v2.getVersion());

        KeyVersion current = keyManager.getCurrentVersion();
        assertEquals(2, current.getVersion());
    }

    @Test
    @DisplayName("Test KeyManager rotates multiple times")
    public void testMultipleRotations() {
        for (int i = 1; i <= 5; i++) {
            KeyVersion v = keyManager.getCurrentVersion();
            assertEquals(i, v.getVersion());
            if (i < 5) keyManager.rotateKey();
        }
    }

    @Test
    @DisplayName("Test KeyManager derives session keys deterministically")
    public void testDeterministicKeyDerivation() throws Exception {
        SecretKey k1 = keyManager.deriveSessionKey(1);
        SecretKey k2 = keyManager.deriveSessionKey(1);

        assertArrayEquals(k1.getEncoded(), k2.getEncoded());
    }

    @Test
    @DisplayName("Test KeyManager different versions produce different keys")
    public void testDifferentVersionsProduceDifferentKeys() throws Exception {
        SecretKey k1 = keyManager.deriveSessionKey(1);
        SecretKey k2 = keyManager.deriveSessionKey(2);

        assertFalse(Arrays.equals(k1.getEncoded(), k2.getEncoded()));
    }

    @Test
    @DisplayName("Test KeyManager gets previous version after rotation")
    public void testGetPreviousVersion() {
        keyManager.rotateKey();

        KeyVersion prev = keyManager.getPreviousVersion();
        assertNotNull(prev);
        assertEquals(1, prev.getVersion());
    }

    @Test
    @DisplayName("Test KeyManager persists keystore to disk")
    public void testPersistKeystore() throws IOException {
        keyManager.rotateKey();
        keyManager.rotateKey();

        KeyManager km2 = new KeyManager(keyStorePath.toString());
        KeyVersion current = km2.getCurrentVersion();
        assertEquals(3, current.getVersion());
    }

    /**
     * FIXED: Test that deleteKeysOlderThan properly removes old versions
     *
     * Before fix: Was checking sessionKeys.size() which would be wrong
     * After fix: Check that deleted versions cannot be accessed
     */
    @Test
    @DisplayName("Test KeyManager deletes old keys")
    public void testDeleteKeysOlderThan() {
        // Create: v1 (initial), v2, v3, v4
        keyManager.rotateKey();  // v2
        keyManager.rotateKey();  // v3
        keyManager.rotateKey();  // v4

        KeyVersion current = keyManager.getCurrentVersion();
        assertEquals(4, current.getVersion());

        // Delete all versions < 2 (deletes v1)
        keyManager.deleteKeysOlderThan(2);

        // Current version should still be v4
        KeyVersion currentAfterDelete = keyManager.getCurrentVersion();
        assertEquals(4, currentAfterDelete.getVersion());

        // v1 should be deleted and inaccessible
        SecretKey v1Key = keyManager.getSessionKey(1);
        assertNull(v1Key, "Version 1 should be deleted and return null");

        // v2, v3, v4 should still be accessible
        SecretKey v2Key = keyManager.getSessionKey(2);
        assertNotNull(v2Key, "Version 2 should still exist");

        SecretKey v3Key = keyManager.getSessionKey(3);
        assertNotNull(v3Key, "Version 3 should still exist");

        SecretKey v4Key = keyManager.getSessionKey(4);
        assertNotNull(v4Key, "Version 4 should still exist");
    }
}