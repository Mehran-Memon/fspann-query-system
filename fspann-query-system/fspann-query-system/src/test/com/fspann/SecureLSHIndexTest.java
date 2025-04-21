package com.fspann;

import com.fspann.keymanagement.KeyManager;
import com.fspann.keymanagement.KeyVersionManager;
import com.fspann.index.SecureLSHIndex;
import com.fspann.query.EncryptedPoint;
import com.fspann.query.QueryToken;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class SecureLSHIndexTest {

    @Test
    public void testAddAndRetrieve() throws Exception {
        // Setup
        KeyManager keyManager = new KeyManager(1000);
        KeyVersionManager keyVersionManager = new KeyVersionManager(keyManager, 1000); // Initialize KeyVersionManager
        SecretKey currentKey = keyVersionManager.getCurrentKey(); // Get the current key from KeyVersionManager

        // Provide dummy data for initial data in SecureLSHIndex constructor
        List<double[]> initialData = new ArrayList<>();
        initialData.add(new double[128]); // Adding a dummy 128-dimensional vector
        SecureLSHIndex index = new SecureLSHIndex(5, currentKey, initialData); // Update constructor for SecureLSHIndex

        // Test: Add a vector
        double[] vector = new double[128];  // Example vector
        String id = "vector_1";
        index.add(id, vector, false);

        // Retrieve the vector by querying
        QueryToken queryToken = new QueryToken(List.of(1), null, 1, "epoch_v" + keyVersionManager.getTimeVersion());
        List<EncryptedPoint> nearestNeighbors = index.findNearestNeighborsEncrypted(queryToken);

        // Assert that the vector is in the index
        assertFalse(nearestNeighbors.isEmpty(), "The vector should be retrieved.");
        assertEquals(id, nearestNeighbors.get(0).getPointId(), "The retrieved vector's ID should match.");
    }

    @Test
    public void testRemove() throws Exception {
        // Setup
        KeyManager keyManager = new KeyManager(1000);
        KeyVersionManager keyVersionManager = new KeyVersionManager(keyManager, 1000); // Initialize KeyVersionManager
        SecretKey currentKey = keyVersionManager.getCurrentKey(); // Get the current key from KeyVersionManager

        // Provide dummy data for initial data in SecureLSHIndex constructor
        List<double[]> initialData = new ArrayList<>();
        initialData.add(new double[128]); // Adding a dummy 128-dimensional vector
        SecureLSHIndex index = new SecureLSHIndex(5, currentKey, initialData);

        // Test: Add and then remove a vector
        double[] vector = new double[128];  // Example vector
        String id = "vector_2";
        index.add(id, vector, false);

        // Remove the vector
        index.remove(id);

        // Test: Try to find the removed vector
        QueryToken queryToken = new QueryToken(List.of(1), null, 1, "epoch_v" + keyVersionManager.getTimeVersion());
        List<EncryptedPoint> nearestNeighbors = index.findNearestNeighborsEncrypted(queryToken);

        // Assert that the vector has been removed
        assertTrue(nearestNeighbors.isEmpty(), "The vector should not be found after removal.");
    }

    @Test
    public void testRehash() throws Exception {
        // Setup
        KeyManager keyManager = new KeyManager(1000);
        KeyVersionManager keyVersionManager = new KeyVersionManager(keyManager, 1000); // Initialize KeyVersionManager
        SecretKey currentKey = keyVersionManager.getCurrentKey(); // Get the current key from KeyVersionManager

        // Provide dummy data for initial data in SecureLSHIndex constructor
        List<double[]> initialData = new ArrayList<>();
        initialData.add(new double[128]); // Adding a dummy 128-dimensional vector
        SecureLSHIndex index = new SecureLSHIndex(5, currentKey, initialData);

        // Test: Add a vector
        double[] vector = new double[128];  // Example vector
        String id = "vector_3";
        index.add(id, vector, false);

        // Get the current encryption key before rehash
        SecretKey keyBeforeRehash = keyVersionManager.getCurrentKey();

        // Rotate keys (simulate rehashing)
        keyVersionManager.rotateKeys(); // Simulate key rotation
        index.rehash(keyVersionManager.getKeyManager(), "epoch_v" + keyVersionManager.getTimeVersion());

        // Get the encryption key after rehash
        SecretKey keyAfterRehash = keyVersionManager.getCurrentKey();

        // Assert that the key has changed after rehashing
        assertNotEquals(keyBeforeRehash, keyAfterRehash, "The key should have changed after rehashing.");
    }
}
