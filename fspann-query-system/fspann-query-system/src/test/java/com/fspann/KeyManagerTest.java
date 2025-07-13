package com.fspann;

import com.fspann.keymanagement.KeyManager;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;

import static org.junit.jupiter.api.Assertions.*;

public class KeyManagerTest {

    @Test
    public void testKeyGeneration() {
        KeyManager keyManager = new KeyManager(1000);
        SecretKey currentKey = keyManager.getCurrentKey();
        SecretKey previousKey = keyManager.getPreviousKey();

        // Assert that the current key and previous key are different
        assertNotNull(currentKey);
        assertNotNull(previousKey);
        assertNotEquals(currentKey, previousKey);
    }
}
