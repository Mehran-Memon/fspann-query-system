package java.com.fspann;

import java.com.fspann.keymanagement.KeyManager;
import java.com.fspann.index.SecureLSHIndex;
import java.com.fspann.query.EncryptedPoint;
import java.com.fspann.query.QueryToken;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class SecureLSHIndexTest {

    @Test
    public void testAddAndRetrieve() throws Exception {
        // Setup
        KeyManager keyManager = new KeyManager(1000); // Initialize key manager with rotation interval
        SecretKey currentKey = keyManager.getCurrentKey(); // Get the current key from KeyManager

        // Provide dummy data for initial data in SecureLSHIndex constructor
        List<double[]> initialData = new ArrayList<>();
        initialData.add(new double[128]); // Adding a dummy 128-dimensional vector
        SecureLSHIndex index = new SecureLSHIndex(128, 5, 10, currentKey, 1000, 100, initialData);

        // Test: Add a vector
        double[] vector = new double[128];  // Example vector
        String id = "vector_1";
        index.add(id, vector, false);

        // Retrieve the vector by querying
        QueryToken queryToken = new QueryToken(List.of(1), null, 1, "epoch_" + keyManager.getTimeVersion());
        List<EncryptedPoint> nearestNeighbors = index.findNearestNeighborsEncrypted(queryToken);

        // Assert that the vector is in the index
        assertFalse(nearestNeighbors.isEmpty(), "The vector should be retrieved.");
        assertEquals(id, nearestNeighbors.getFirst().getPointId(), "The retrieved vector's ID should match.");
    }

    @Test
    public void testRemove() throws Exception {
        // Setup
        KeyManager keyManager = new KeyManager(1000);
        SecretKey currentKey = keyManager.getCurrentKey(); // Get the current key from KeyManager

        // Provide dummy data for initial data in SecureLSHIndex constructor
        List<double[]> initialData = new ArrayList<>();
        initialData.add(new double[128]); // Adding a dummy 128-dimensional vector
        SecureLSHIndex index = new SecureLSHIndex(128, 5, 10, currentKey, 1000, 100, initialData);

        // Test: Add and then remove a vector
        double[] vector = new double[128];  // Example vector
        String id = "vector_2";
        index.add(id, vector, false);

        // Remove the vector
        index.remove(id);

        // Test: Try to find the removed vector
        QueryToken queryToken = new QueryToken(List.of(1), null, 1, "epoch_" + keyManager.getTimeVersion());
        List<EncryptedPoint> nearestNeighbors = index.findNearestNeighborsEncrypted(queryToken);

        // Assert that the vector has been removed
        assertTrue(nearestNeighbors.isEmpty(), "The vector should not be found after removal.");
    }

    @Test
    public void testRehash() throws Exception {
        // Setup
        KeyManager keyManager = new KeyManager(1000);
        SecretKey currentKey = keyManager.getCurrentKey(); // Get the current key from KeyManager

        // Provide dummy data for initial data in SecureLSHIndex constructor
        List<double[]> initialData = new ArrayList<>();
        initialData.add(new double[128]); // Adding a dummy 128-dimensional vector
        SecureLSHIndex index = new SecureLSHIndex(128, 5, 10, currentKey, 1000, 100, initialData);

        // Test: Add a vector
        double[] vector = new double[128];  // Example vector
        String id = "vector_3";
        index.add(id, vector, false);

        // Get the current encryption key before rehash
        SecretKey keyBeforeRehash = keyManager.getCurrentKey();

        // Rotate keys (simulate rehashing)
        keyManager.rotateAllKeys(null, null);
        index.rehash(keyManager, "epoch_" + keyManager.getTimeVersion());

        // Get the encryption key after rehash
        SecretKey keyAfterRehash = keyManager.getCurrentKey();

        // Assert that the key has changed after rehashing
        assertNotEquals(keyBeforeRehash, keyAfterRehash, "The key should have changed after rehashing.");
    }
}
