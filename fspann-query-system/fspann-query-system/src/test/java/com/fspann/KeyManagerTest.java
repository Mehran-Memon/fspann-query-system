package java.com.fspann;

import java.com.fspann.keymanagement.KeyManager;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;

import static org.junit.jupiter.api.Assertions.*;

public class KeyManagerTest {

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
        KeyManager keyManager = new KeyManager(2); // Rotate keys every 2 operations
        SecretKey currentKey = keyManager.getCurrentKey();

        // Perform an operation
        keyManager.rotateAllKeys(null, null); // Simulate rotation (no need for real encrypted data)

        // After rotation, the current key should be different
        SecretKey rotatedKey = keyManager.getCurrentKey();
        assertNotEquals(currentKey, rotatedKey, "Keys should be rotated");

        // Verify previous key
        assertEquals(currentKey, keyManager.getPreviousKey(), "Previous key should match the previous current key");
    }

    @Test
    public void testKeyVersioning() throws Exception {
        KeyManager keyManager = new KeyManager(2); // Rotate keys every 2 operations

        // Generate and retrieve keys for versioning
        SecretKey keyV1 = keyManager.getSessionKey("key_v1");
        assertNotNull(keyV1, "Key for version v1 should not be null");

        // Perform rotation and verify key v2
        keyManager.rotateAllKeys(null, null);
        SecretKey keyV2 = keyManager.getSessionKey("key_v2");
        assertNotNull(keyV2, "Key for version v2 should not be null");

        // Ensure the keys for version v1 and v2 are different
        assertNotEquals(keyV1, keyV2, "Versioned keys should be different");
    }
}
