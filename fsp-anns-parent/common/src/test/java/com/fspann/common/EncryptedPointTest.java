package com.fspann.common;

import org.junit.jupiter.api.Test;
import java.util.Arrays;
import static org.junit.jupiter.api.Assertions.*;

class EncryptedPointTest {
    @Test
    void gettersReturnCorrectValuesAndClone() {
        byte[] iv = {0, 1, 2};
        byte[] ciphertext = {3, 4, 5};
        EncryptedPoint point = new EncryptedPoint("vec1", 1, iv, ciphertext, 1, 128);
        assertEquals("vec1", point.getId());
        assertEquals(1, point.getShardId()); // Line 36: expect 1
        assertEquals(1, point.getVersion());
        assertEquals(128, point.getVectorLength());
        assertArrayEquals(iv, point.getIv());
        assertArrayEquals(ciphertext, point.getCiphertext());
        // Verify cloning
        iv[0] = 9;
        assertArrayEquals(new byte[]{0, 1, 2}, point.getIv());
        ciphertext[0] = 9;
        assertArrayEquals(new byte[]{3, 4, 5}, point.getCiphertext());
    }

    @Test
    void constructorClonesInputArrays() {
        byte[] iv = {0, 1, 2};
        byte[] ciphertext = {3, 4, 5};
        EncryptedPoint point = new EncryptedPoint("vec1", 1, iv, ciphertext, 1, 128);
        iv[0] = 7;
        assertArrayEquals(new byte[]{0, 1, 2}, point.getIv()); // Line 49: expect original 0
        ciphertext[0] = 7;
        assertArrayEquals(new byte[]{3, 4, 5}, point.getCiphertext());
    }

    @Test
    void nullIvOrCiphertextThrows() {
        assertThrows(NullPointerException.class, () -> new EncryptedPoint("vec1", 1, null, new byte[]{3, 4, 5}, 1, 128));
        assertThrows(NullPointerException.class, () -> new EncryptedPoint("vec1", 1, new byte[]{0, 1, 2}, null, 1, 128));
        assertThrows(NullPointerException.class, () -> new EncryptedPoint(null, 1, new byte[]{0, 1, 2}, new byte[]{3, 4, 5}, 1, 128));
    }
}