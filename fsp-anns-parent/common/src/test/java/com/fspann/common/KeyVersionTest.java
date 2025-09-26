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
        KeyGenerator kg = KeyGenerator.getInstance("AES");
        kg.init(128);
        SecretKey key = kg.generateKey();

        KeyVersion kv = new KeyVersion(7, key);

        assertEquals(7, kv.getVersion());
        assertNotNull(kv.getKey());
        assertArrayEquals(key.getEncoded(), kv.getKey().getEncoded(),
                "Returned key material should match the original");
    }

}