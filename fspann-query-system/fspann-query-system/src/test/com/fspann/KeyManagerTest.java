package com.fspann;

import com.fspann.ForwardSecureANNSystem;
import com.fspann.keymanagement.KeyManager;
import com.fspann.keymanagement.KeyVersionManager;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class KeyManagerTest {
    ForwardSecureANNSystem system;

    @Test
    public void testKeyGeneration() {
        KeyManager keyManager = new KeyManager(5000);
        SecretKey currentKey = keyManager.getCurrentKey();
        SecretKey previousKey = keyManager.getPreviousKey();

        // Assert that the current key and previous key are different
        assertNotNull(currentKey);
        assertNotNull(previousKey);
        assertNotEquals(currentKey, previousKey);
    }

    @Test
    public void testKeyRotation() throws Exception {
        KeyManager keyManager = new KeyManager(1000);
        KeyVersionManager keyVersionManager = new KeyVersionManager(keyManager,1000);

        // Assuming encryptedDataMap is a map of encrypted data with their ids
        List<String> ids = new ArrayList<>(system.encryptedDataStore.keySet());  // Access encryptedDataStore from system
        Map<String, byte[]> encryptedDataMap = new HashMap<>();

        for (String id : ids) {
            encryptedDataMap.put(id, system.encryptedDataStore.get(id).getCiphertext());
        }

        keyVersionManager.rotateKeys(); // Rotate the keys
        // Ensure that re-encryption is handled properly after key rotation
        keyManager.rotateAllKeys(ids, encryptedDataMap);
    }

    @Test
    public void testKeyVersioning() throws Exception {
        KeyManager keyManager = new KeyManager(1000);
        KeyVersionManager keyVersionManager = new KeyVersionManager(keyManager, 1000);

        String context = "key_v" + keyVersionManager.getTimeVersion(); // Correct context
        SecretKey sessionKey = keyManager.getSessionKey(context);

        assertNotNull(sessionKey, "Session key should be valid for the given context");
    }
}
