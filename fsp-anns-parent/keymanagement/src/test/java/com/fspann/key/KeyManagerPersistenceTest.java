package com.fspann.key;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@DisplayName("KeyManager Persistence Tests")
public class KeyManagerPersistenceTest {

    private Path keyStorePath;

    @TempDir
    Path tempDir;

    @BeforeEach
    public void setUp() {
        keyStorePath = tempDir.resolve("keystore.blob");
    }

    @Test
    @DisplayName("Test KeyManager saves and loads keystore")
    public void testSaveAndLoad() throws IOException {
        KeyManager km1 = new KeyManager(keyStorePath.toString());
        km1.rotateKey();
        km1.rotateKey();

        int v1 = km1.getCurrentVersion().getVersion();
        assertEquals(3, v1);

        KeyManager km2 = new KeyManager(keyStorePath.toString());
        int v2 = km2.getCurrentVersion().getVersion();

        assertEquals(v1, v2);
    }

    @Test
    @DisplayName("Test KeyManager handles corrupted keystore gracefully")
    public void testCorruptedKeystore() throws IOException {
        KeyManager km1 = new KeyManager(keyStorePath.toString());
        Files.write(keyStorePath, "corrupted data".getBytes());

        assertThrows(IOException.class, () -> new KeyManager(keyStorePath.toString()));
    }
}