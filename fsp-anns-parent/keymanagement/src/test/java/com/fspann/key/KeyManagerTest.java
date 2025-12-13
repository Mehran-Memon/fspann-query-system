package com.fspann.key;

import com.fspann.common.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

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

    @Test
    @DisplayName("Test KeyManager deletes old keys")
    public void testDeleteKeysOlderThan() {
        keyManager.rotateKey();
        keyManager.rotateKey();
        keyManager.rotateKey();

        keyManager.deleteKeysOlderThan(2);

        KeyVersion current = keyManager.getCurrentVersion();
        assertEquals(3, current.getVersion());
    }
}