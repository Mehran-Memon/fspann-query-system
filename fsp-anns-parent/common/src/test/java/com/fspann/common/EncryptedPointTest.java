package com.fspann.common;

import org.junit.jupiter.api.Test;
import com.fspann.common.QueryToken;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class EncryptedPointTest {
    @Test
    void gettersReturnCorrectValuesAndClone() {
        String id = "pt1";
        int shardId = 5;
        byte[] iv = new byte[]{1, 2, 3};
        byte[] ct = new byte[]{4, 5, 6, 7};
        EncryptedPoint ep = new EncryptedPoint(id, shardId, iv, ct, 1,2);

        assertEquals(id, ep.getId());
        assertEquals(shardId, ep.getShardId());
        assertArrayEquals(iv, ep.getIv());
        assertArrayEquals(ct, ep.getCiphertext());

        // modify returned arrays
        byte[] returnedIv = ep.getIv();
        byte[] returnedCt = ep.getCiphertext();
        returnedIv[0] = 9;
        returnedCt[0] = 9;
        // ensure original data inside EncryptedPoint is unchanged
        assertEquals(1, ep.getIv()[0]);
        assertEquals(4, ep.getCiphertext()[0]);
    }

    @Test
    void constructorClonesInputArrays() {
        byte[] iv = new byte[]{7, 8};
        byte[] ct = new byte[]{9, 10};
        EncryptedPoint ep = new EncryptedPoint("id", 0, iv, ct, 1,2);
        // modify original arrays
        iv[0] = 0;
        ct[0] = 0;
        // EncryptedPoint should have cloned the inputs
        assertEquals(7, ep.getIv()[0]);
        assertEquals(9, ep.getCiphertext()[0]);
    }

    @Test
    void nullIvOrCiphertextThrows() {
        // Passing null for iv should throw NPE
        assertThrows(NullPointerException.class, () -> new EncryptedPoint("id", 0, null, new byte[]{1}, 1, 2 ));
        // Passing null for ciphertext should throw NPE
        assertThrows(NullPointerException.class, () -> new EncryptedPoint("id", 0, new byte[]{1}, null, 1, 2));
    }}

