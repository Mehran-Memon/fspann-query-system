package com.key;

import com.fspann.common.KeyVersion;
import com.fspann.key.KeyManager;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import com.fspann.common.FsPaths;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class KeyManagerTest {

    private KeyManager keyManager;
    private Path keyFile;

    @BeforeEach
    void setUp(@TempDir Path tempDir) throws IOException {
        keyFile = tempDir.resolve("keys.ser");
        keyManager = new KeyManager(keyFile.toString());
    }

    @AfterEach
    void tearDown() throws Exception {
        keyManager = null;
        System.gc();
        Thread.sleep(150); // Let JVM release file handles
    }

    @Test
    void testKeyRotation() {
        KeyVersion v1 = keyManager.getCurrentVersion();
        KeyVersion v2 = keyManager.rotateKey();

        assertNotNull(v1);
        assertNotNull(v2);
        assertNotEquals(v1.getVersion(), v2.getVersion());
        assertEquals(v1.getVersion() + 1, v2.getVersion());
        assertNotNull(v2.getKey());
        assertTrue(v2.getKey().getEncoded().length > 0);
    }

    @Test
    void testLoadInvalidKeyStore() throws Exception {
        // Create and write corrupted content
        Path badFile = Files.createTempFile("bad_keys", ".ser");
        try (var out = Files.newOutputStream(badFile)) {
            out.write("corrupted-content".getBytes(StandardCharsets.UTF_8));
        }

        // Assert KeyManager fails to load the corrupted file
        assertThrows(IOException.class, () -> new KeyManager(badFile.toString()));

        // Wait for Windows to release the file handle
        System.gc();                  // Suggest GC to help with releasing file locks
        Thread.sleep(150);           // Small delay to ensure OS unlocks the file

        // Clean up manually
        try {
            Files.deleteIfExists(badFile);
        } catch (IOException e) {
            System.err.println("Cleanup failed: " + e.getMessage());
        }
    }

    @Test
    void testUniqueKeyMaterialPerRotation() {
        KeyVersion v1 = keyManager.getCurrentVersion();
        KeyVersion v2 = keyManager.rotateKey();

        assertNotEquals(v1.getVersion(), v2.getVersion(), "Key version should change after rotation.");
        assertFalse(Arrays.equals(v1.getKey().getEncoded(), v2.getKey().getEncoded()),
                "Key material must differ after rotation for security.");
    }

    @Test
    void testKeySerializationAndRestoration() throws IOException, ClassNotFoundException {
        KeyVersion original = keyManager.rotateKey();

        KeyManager reloadedManager = new KeyManager(keyFile.toString());
        KeyVersion loaded = reloadedManager.getCurrentVersion();

        assertEquals(original.getVersion(), loaded.getVersion(), "Version must persist after reloading.");
        assertArrayEquals(original.getKey().getEncoded(), loaded.getKey().getEncoded(),
                "Key material must match after reloading.");
    }

    @Test
    void testKeyTamperingDetection() throws Exception {
        Files.write(keyFile, "tampered-content".getBytes());

        assertThrows(IOException.class, () -> new KeyManager(keyFile.toString()),
                "Tampering should be detected via StreamCorruptedException or IOException.");

        // Help JVM and Windows clean up before TempDir tries deletion
        System.gc();
        Thread.sleep(150);
    }

    @Test
    void testFileMissingOnDeserialization() throws IOException {
        Path missingPath = keyFile.resolveSibling("nonexistent.ser");
        assertFalse(Files.exists(missingPath));
        KeyManager fresh = new KeyManager(missingPath.toString());
        assertNotNull(fresh.getCurrentVersion());
    }

    @Test
    void testMultipleRotationsProduceDifferentKeys() {
        byte[] k1 = keyManager.getCurrentVersion().getKey().getEncoded();
        byte[] k2 = keyManager.rotateKey().getKey().getEncoded();
        byte[] k3 = keyManager.rotateKey().getKey().getEncoded();

        assertFalse(Arrays.equals(k1, k2));
        assertFalse(Arrays.equals(k2, k3));
        assertFalse(Arrays.equals(k1, k3));
    }

    @Test
    void testConcurrentKeyRotation() throws InterruptedException {
        int start = keyManager.getCurrentVersion().getVersion();

        ExecutorService ex = Executors.newFixedThreadPool(4);
        IntStream.range(0, 100).forEach(i -> ex.submit(() -> keyManager.rotateKey()));
        ex.shutdown();
        assertTrue(ex.awaitTermination(5, TimeUnit.SECONDS));

        assertEquals(start + 100, keyManager.getCurrentVersion().getVersion());
    }

    @Test
    void testNullKeyFileUsesDefaultPath(@TempDir Path tempDir) throws Exception {
        System.setProperty(FsPaths.BASE_DIR_PROP, tempDir.toString());
        try {
            KeyManager km = new KeyManager(null);
            assertTrue(Files.exists(FsPaths.keyStoreFile()), "keystore should be created at default path");
            assertNotNull(km.getCurrentVersion());
        } finally {
            System.clearProperty(FsPaths.BASE_DIR_PROP);
        }
    }

    @Test
    void testBlankKeyFileUsesDefaultPath(@TempDir Path tempDir) throws Exception {
        System.setProperty(FsPaths.BASE_DIR_PROP, tempDir.toString());
        try {
            KeyManager km = new KeyManager("   ");
            Path ks = FsPaths.keyStoreFile();
            assertTrue(Files.exists(ks));
            int v0 = km.getCurrentVersion().getVersion();
            km.rotateKey();
            assertTrue(Files.size(ks) > 0);
            assertEquals(v0 + 1, km.getCurrentVersion().getVersion());
        } finally {
            System.clearProperty(FsPaths.BASE_DIR_PROP);
        }
    }

    @Test
    void testLoadInvalidKeyStore_allowsStrictOrRecovery() throws Exception {
        Path badFile = Files.createTempFile("bad_keys", ".ser");
        Files.writeString(badFile, "corrupted-content", StandardCharsets.UTF_8);

        try {
            // Either throws (strict) or recovers by creating a fresh store (lenient)
            try {
                new KeyManager(badFile.toString());
                // lenient path: just ensure it now has a usable version
                KeyManager km = new KeyManager(badFile.toString());
                assertNotNull(km.getCurrentVersion());
            } catch (IOException expected) {
                // strict path: OK as well
            }
        } finally {
            System.gc();
            Thread.sleep(150);
            Files.deleteIfExists(badFile);
        }
    }
}
