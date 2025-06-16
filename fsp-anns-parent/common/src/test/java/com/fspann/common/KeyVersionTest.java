package com.fspann.common;

import org.junit.jupiter.api.Test;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.security.NoSuchAlgorithmException;

import static org.junit.jupiter.api.Assertions.*;

class KeyVersionTest {
    @Test
    void gettersReturnCorrectValues() throws NoSuchAlgorithmException {
        // Generate AES key
        KeyGenerator kg = KeyGenerator.getInstance("AES");
        kg.init(128);
        SecretKey key = kg.generateKey();

        // Create KeyVersion with the current constructor
        KeyVersion kv = new KeyVersion(7, key);

        // Assertions
        assertEquals(7, kv.getVersion());
        assertNotNull(kv.getKey()); // Ensure getKey() returns a non-null value
        assertArrayEquals(key.getEncoded(), ((SecretKeySpec) kv.getKey()).getEncoded(),
                "Reconstructed key should match the original encoded key");
    }
}