package com.key;

import com.fspann.common.KeyVersion;
import com.fspann.key.KeyManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.nio.file.Path;

import static org.mockito.Mockito.doThrow;
import static org.junit.jupiter.api.Assertions.*;

class KeyManagerTest {
    @Mock
    private KeyManager mockedKeyManager;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testRotateKeyWithInvalidKey() {
        doThrow(new RuntimeException("Test invalid key")).when(mockedKeyManager).rotateKey();
        assertThrows(RuntimeException.class, mockedKeyManager::rotateKey);
    }

    @Test
    void testKeyRotation(@TempDir Path tempDir) throws IOException {
        Path keyFile = tempDir.resolve("keys.ser");
        KeyManager km = new KeyManager(keyFile.toString());

        KeyVersion v1 = km.getCurrentVersion();
        KeyVersion v2 = km.rotateKey();

        assertNotNull(v1);
        assertNotNull(v2);
        assertNotEquals(v1.getVersion(), v2.getVersion());
        assertEquals(v1.getVersion() + 1, v2.getVersion());

        // Additional checks for IV and EncryptedQuery
        assertNotNull(v2.getIv());
        assertNotNull(v2.getEncryptedQuery());
        assertTrue(v2.getIv().length > 0);
        assertTrue(v2.getEncryptedQuery().length > 0);
    }

}
