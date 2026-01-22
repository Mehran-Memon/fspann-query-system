package com.fspann.it;

import com.fspann.common.EncryptedPoint;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SuperSecurityLifecycleIT extends BaseUnifiedIT {

    @Disabled
    @Test
    @DisplayName("Touched vectors migrate after key rotation")
    void selectiveReencryptionWorks() throws Exception {
        indexClusteredData(20);

        // Ensure data is flushed
        system.flushAll();
        metadata.flush();
        Thread.sleep(100);  // Give RocksDB time

        double[] query = new double[DIM];
        Arrays.fill(query, 5.0);

        system.getQueryServiceImpl()
                .search(system.createToken(query, 5, DIM));

        // Re-initialize tracking
        keyService.initializeUsageTracking();

        keyService.rotateKeyOnly();
        keyService.reEncryptAll();

        system.flushAll();
        metadata.flush();
        Thread.sleep(100);

        // Get all points
        List<EncryptedPoint> allPoints = metadata.getAllEncryptedPoints();

        // FIXED: Correct assertion
        assertTrue(allPoints != null && !allPoints.isEmpty(),
                "Should have encrypted points after re-encryption");

        EncryptedPoint ep = allPoints.get(0);
        assertNotNull(ep, "First encrypted point should not be null");

        // Verify it's on the new key version
        int currentVersion = keyService.getCurrentVersion().getVersion();
        assertEquals(currentVersion, ep.getVersion(),
                "Re-encrypted point should use current key version");
    }
    @Disabled
    @Test
    @DisplayName("Old key cannot decrypt re-encrypted data")
    void oldKeyBreaks() throws IOException, ClassNotFoundException, InterruptedException {
        indexClusteredData(10);

        system.flushAll();
        metadata.flush();
        Thread.sleep(100);

        SecretKey oldKey = keyService.getCurrentVersion().getKey();

        keyService.initializeUsageTracking();
        keyService.rotateKeyOnly();
        keyService.reEncryptAll();

        system.flushAll();
        metadata.flush();
        Thread.sleep(100);

        // Use proper numeric ID
        EncryptedPoint ep = metadata.loadEncryptedPoint("0");
        assertNotNull(ep, "Encrypted point v0 should exist");

        assertThrows(
                RuntimeException.class,
                () -> crypto.decryptFromPoint(ep, oldKey),
                "Old key should fail to decrypt re-encrypted point"
        );
    }
}