package com.key;

import com.fspann.common.KeyVersion;
import com.fspann.key.KeyManager;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class KeyManagerTest {

    private KeyManager km;
    private Path keyFile;

    @BeforeEach
    void setup(@TempDir Path dir) throws Exception {
        keyFile = dir.resolve("keystore.ser");
        km = new KeyManager(keyFile.toString());
    }

    @AfterEach
    void tearDown() {
        km = null;
        System.gc();
    }

    // -------------------------------------------------------------
    // BASIC ROTATION + VERSIONING
    // -------------------------------------------------------------

    @Test
    void rotatesVersionCorrectly() {
        int v0 = km.getCurrentVersion().getVersion();
        KeyVersion v1 = km.rotateKey();
        assertEquals(v0 + 1, v1.getVersion());
    }

    @Test
    void rotatingProducesDistinctKeys() {
        SecretKey k0 = km.getCurrentVersion().getKey();
        SecretKey k1 = km.rotateKey().getKey();
        SecretKey k2 = km.rotateKey().getKey();

        assertFalse(Arrays.equals(k0.getEncoded(), k1.getEncoded()));
        assertFalse(Arrays.equals(k1.getEncoded(), k2.getEncoded()));
        assertFalse(Arrays.equals(k0.getEncoded(), k2.getEncoded()));
    }

    // -------------------------------------------------------------
    // SERIALIZATION SAFETY
    // -------------------------------------------------------------

    @Test
    void corruptedFileFailsToLoad() throws Exception {
        Path bad = Files.createTempFile("corrupt", ".ser");
        Files.writeString(bad, "garbage-data", StandardCharsets.UTF_8);

        assertThrows(IOException.class, () -> new KeyManager(bad.toString()));
    }

    @Test
    void missingFileCreatesFreshKeystore() throws Exception {
        Path missing = keyFile.resolveSibling("absent.ser");
        KeyManager km2 = new KeyManager(missing.toString());
        assertNotNull(km2.getCurrentVersion());
        assertEquals(1, km2.getCurrentVersion().getVersion());
    }

    @Test
    void keysPersistAndReloadCorrectly() throws Exception {
        KeyVersion vOriginal = km.rotateKey();

        KeyManager kmReloaded = new KeyManager(keyFile.toString());
        KeyVersion vLoaded = kmReloaded.getCurrentVersion();

        assertEquals(vOriginal.getVersion(), vLoaded.getVersion());
        assertArrayEquals(vOriginal.getKey().getEncoded(), vLoaded.getKey().getEncoded());
    }

    // -------------------------------------------------------------
    // CONCURRENCY SAFETY
    // -------------------------------------------------------------

    @Test
    void rotationIsThreadSafe() throws Exception {
        int base = km.getCurrentVersion().getVersion();

        ExecutorService exec = Executors.newFixedThreadPool(8);
        IntStream.range(0, 200).forEach(i -> exec.submit(km::rotateKey));
        exec.shutdown();
        exec.awaitTermination(5, TimeUnit.SECONDS);

        assertEquals(base + 200, km.getCurrentVersion().getVersion());
    }

    // -------------------------------------------------------------
    // ENTROPY CHECK (OPTIONAL BUT RECOMMENDED)
    // -------------------------------------------------------------

    @Test
    void keyMaterialNotAllZeros() {
        byte[] k = km.getCurrentVersion().getKey().getEncoded();
        assertFalse(Arrays.equals(k, new byte[k.length]), "Key should not be zeroed material");
    }
}
