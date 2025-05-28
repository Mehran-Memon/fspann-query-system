// File: src/test/java/com/fspann/common/QueryTokenTest.java
package com.fspann.common;

import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class QueryTokenTest {
    @Test
    void constructorValidatesAndClones() {
        // Use a mutable list so we can modify it in the immutability check
        List<Integer> buckets = new ArrayList<>(List.of(1, 2, 3));
        byte[] iv  = new byte[]{0x00};
        byte[] qry = new byte[]{9, 8};
        QueryToken token = new QueryToken(buckets, iv, qry, 4, 2, "ctx");

        assertEquals(buckets, token.getCandidateBuckets());
        assertArrayEquals(iv,  token.getIv());
        assertArrayEquals(qry, token.getEncryptedQuery());
        assertEquals(4, token.getTopK());
        assertEquals(2, token.getNumTables());
        assertEquals("ctx", token.getEncryptionContext());

        // immutability: modifying our original list must NOT affect the token's view
        buckets.set(0, 99);
        assertNotEquals(buckets, token.getCandidateBuckets());

        iv[0] = 7;
        assertNotEquals(iv[0], token.getIv()[0]);

        qry[1] = 5;
        assertNotEquals(qry[1], token.getEncryptedQuery()[1]);
    }

    @Test
    void constructorRejectsNullOrEmpty() {
        byte[] iv      = new byte[]{0x00};
        byte[] payload = new byte[]{0x01};

        // null buckets
        assertThrows(IllegalArgumentException.class, () ->
                new QueryToken(null, iv, payload, 1, 1, "ctx")
        );

        // empty buckets
        assertThrows(IllegalArgumentException.class, () ->
                new QueryToken(List.of(), iv, payload, 1, 1, "ctx")
        );

        // negative topK
        assertThrows(IllegalArgumentException.class, () ->
                new QueryToken(List.of(1), iv, payload, -1, 1, "ctx")
        );

        // zero numTables
        assertThrows(IllegalArgumentException.class, () ->
                new QueryToken(List.of(1), iv, payload, 1, 0, "ctx")
        );
    }

    @Test
    void gettersReturnClonesAndValues() {
        List<Integer> buckets = List.of(2, 5, 7);
        byte[] iv      = new byte[]{0x42};
        byte[] payload = new byte[]{0x13, 0x37};
        QueryToken tok = new QueryToken(buckets, iv, payload, 3, 2, "epoch_1");

        assertEquals(buckets, tok.getCandidateBuckets());
        assertArrayEquals(iv,        tok.getIv());
        assertArrayEquals(payload,   tok.getEncryptedQuery());
        assertEquals(3, tok.getTopK());
        assertEquals(2, tok.getNumTables());
        assertEquals("epoch_1", tok.getEncryptionContext());

        // Modifying returned arrays should not affect internal state
        byte[] ivClone  = tok.getIv();
        byte[] qryClone = tok.getEncryptedQuery();
        ivClone[0] = 0x00;
        qryClone[0] = 0x00;
        assertEquals(0x42, tok.getIv()[0]);
        assertEquals(0x13, tok.getEncryptedQuery()[0]);
    }
}
