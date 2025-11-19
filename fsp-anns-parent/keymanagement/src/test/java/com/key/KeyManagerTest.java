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
        Thread.sleep(150);
    }

    @Test
    void testKeyRotation() {
        KeyVersion v1 = keyManager.getCurrentVersion();
        KeyVersion v2 = keyManager.rotateKey();
        assertNotEquals(v1.getVersion(), v2.getVersion());
    }

    @Test
    void testLoadInvalidKeyStore() throws Exception {
        Path badFile = Files.createTempFile("bad_keys", ".ser");
        Files.writeString(badFile, "corrupted", StandardCharsets.UTF_8);
        assertThrows(IOException.class, () -> new KeyManager(badFile.toString()));
        System.gc();
        Thread.sleep(150);
        Files.deleteIfExists(badFile);
    }

    @Test
    void testUniqueKeyMaterialPerRotation() {
        KeyVersion v1 = keyManager.getCurrentVersion();
        KeyVersion v2 = keyManager.rotateKey();
        assertFalse(Arrays.equals(v1.getKey().getEncoded(), v2.getKey().getEncoded()));
    }

    @Test
    void testKeySerializationAndRestoration() throws Exception {
        KeyVersion original = keyManager.rotateKey();
        KeyManager km2 = new KeyManager(keyFile.toString());
        KeyVersion loaded = km2.getCurrentVersion();
        assertEquals(original.getVersion(), loaded.getVersion());
        assertArrayEquals(original.getKey().getEncoded(), loaded.getKey().getEncoded());
    }

    @Test
    void testFileMissingOnDeserialization() throws IOException {
        Path missing = keyFile.resolveSibling("nope.ser");
        KeyManager km2 = new KeyManager(missing.toString());
        assertNotNull(km2.getCurrentVersion());
    }

    @Test
    void testMultipleRotationsProduceDifferentKeys() {
        byte[] k1 = keyManager.getCurrentVersion().getKey().getEncoded();
        byte[] k2 = keyManager.rotateKey().getKey().getEncoded();
        byte[] k3 = keyManager.rotateKey().getKey().getEncoded();
        assertFalse(Arrays.equals(k1, k2));
        assertFalse(Arrays.equals(k2, k3));
    }

    @Test
    void testConcurrentKeyRotation() throws Exception {
        int start = keyManager.getCurrentVersion().getVersion();
        ExecutorService ex = Executors.newFixedThreadPool(4);
        IntStream.range(0, 100).forEach(i -> ex.submit(keyManager::rotateKey));
        ex.shutdown();
        ex.awaitTermination(5, TimeUnit.SECONDS);
        assertEquals(start + 100, keyManager.getCurrentVersion().getVersion());
    }
}
