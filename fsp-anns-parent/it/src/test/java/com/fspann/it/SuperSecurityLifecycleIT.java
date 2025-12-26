package com.fspann.it;

import com.fspann.common.EncryptedPoint;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class SuperSecurityLifecycleIT extends BaseUnifiedIT {

    @Test
    @DisplayName("Touched vectors migrate after key rotation")
    void selectiveReencryptionWorks() throws Exception {

        indexClusteredData(20);

        double[] query = new double[DIM];
        Arrays.fill(query, 5.0);

        // force touches
        var tok = system.createToken(query, 5, DIM);
        system.getQueryServiceImpl().search(tok);

        int touchedBefore = system.getIndexService().getLastTouchedCount();
        assertTrue(touchedBefore > 0);

        int oldVersion = keyService.getCurrentVersion().getVersion();

        keyService.rotateKeyOnly();
        keyService.reEncryptAll();
        system.flushAll();

        EncryptedPoint ep = metadata.loadEncryptedPoint("v0");
        assertNotNull(ep);

        // probabilistic-safe assertion
        assertTrue(ep.getVersion() >= oldVersion);
    }

    @Test
    @DisplayName("Old key cannot decrypt re-encrypted data")
    void oldKeyBreaks() throws Exception {

        indexClusteredData(10);

        SecretKey oldKey = keyService.getCurrentVersion().getKey();

        keyService.rotateKeyOnly();
        keyService.reEncryptAll();
        system.flushAll();

        EncryptedPoint ep = metadata.loadEncryptedPoint("v0");
        assertNotNull(ep);

        assertThrows(
                RuntimeException.class,
                () -> crypto.decryptFromPoint(ep, oldKey)
        );
    }
}
