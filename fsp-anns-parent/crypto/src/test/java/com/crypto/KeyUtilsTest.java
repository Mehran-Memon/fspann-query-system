package com.crypto;

import com.fspann.common.EncryptedPoint;
import com.fspann.crypto.EncryptionUtils;
import com.fspann.crypto.KeyUtils;
import org.junit.jupiter.api.Test;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.security.SecureRandom;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class KeyUtilsTest {

    @Test
    void fromBytes_validAndInvalid() {
        assertNotNull(KeyUtils.fromBytes(new byte[16]));
        assertNotNull(KeyUtils.fromBytes(new byte[24]));
        assertNotNull(KeyUtils.fromBytes(new byte[32]));
        assertThrows(IllegalArgumentException.class, () -> KeyUtils.fromBytes(new byte[15]));
    }

    @Test
    void tryDecryptWithKeyOnly_successAndFailure() throws Exception {
        KeyGenerator kg = KeyGenerator.getInstance("AES");
        kg.init(256, SecureRandom.getInstanceStrong());
        SecretKey k1 = kg.generateKey();
        SecretKey k2 = kg.generateKey();

        double[] vec = { 4.2, -0.1, 9.9 };
        byte[] iv = EncryptionUtils.generateIV();
        byte[] ct = EncryptionUtils.encryptVector(vec, iv, k1);
        EncryptedPoint pt = new EncryptedPoint("x", 0, iv, ct, 1, vec.length, null);

        Optional<double[]> ok = KeyUtils.tryDecryptWithKeyOnly(pt, k1);
        assertTrue(ok.isPresent());
        assertArrayEquals(vec, ok.get(), 1e-12);

        assertTrue(KeyUtils.tryDecryptWithKeyOnly(pt, k2).isEmpty());
    }
}
