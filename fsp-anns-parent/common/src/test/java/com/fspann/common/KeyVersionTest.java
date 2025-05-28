
// File: src/test/java/com/fspann/common/KeyVersionTest.java
package com.fspann.common;

import org.junit.jupiter.api.Test;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import static org.junit.jupiter.api.Assertions.*;

class KeyVersionTest {
    @Test
    void gettersReturnCorrectValues() throws Exception {
        KeyGenerator kg = KeyGenerator.getInstance("AES");
        kg.init(128);
        SecretKey key = kg.generateKey();
        KeyVersion kv = new KeyVersion(7, key);
        assertEquals(7, kv.getVersion());
        assertSame(key, kv.getSecretKey());
    }
}
