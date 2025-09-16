package com.fspann.common;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class QueryTokenTest {
    @Test
    void constructorValidatesAndClones() {
        List<Integer> buckets = new ArrayList<>(List.of(1, 2, 3));
        byte[] iv            = new byte[] { 0x00 };
        byte[] encryptedQry  = new byte[] { 9, 8 };
        double[] plainQry    = new double[] { 1.0, 2.0 };

        QueryToken token = new QueryToken(
                buckets,
                iv,
                encryptedQry,
                plainQry,
                4,
                2,
                "ctx",
                128,
                0,
                1
        );

        assertEquals(List.of(1, 2, 3), token.getCandidateBuckets());
        assertArrayEquals(iv,          token.getIv());
        assertArrayEquals(encryptedQry, token.getEncryptedQuery());
        assertArrayEquals(plainQry,    token.getPlaintextQuery());
        assertEquals(4,                token.getTopK());
        assertEquals(2,                token.getNumTables());
        assertEquals("ctx",            token.getEncryptionContext());
        assertEquals(128,              token.getDimension());
        assertEquals(0,                token.getShardId());
        assertEquals(1,                token.getVersion());

        // Immutability checks
        buckets.set(0, 99);
        assertNotEquals(buckets, token.getCandidateBuckets());

        iv[0] = 7;
        assertNotEquals(iv[0], token.getIv()[0]);

        encryptedQry[1] = 5;
        assertNotEquals(encryptedQry[1], token.getEncryptedQuery()[1]);

        plainQry[0] = 99.9;
        assertNotEquals(plainQry[0], token.getPlaintextQuery()[0]);
    }

    @Test
    void constructorRejectsInvalidInputs() {
        byte[] iv            = new byte[] { 0x00 };
        byte[] encryptedQry  = new byte[] { 0x01 };
        double[] plainQry    = new double[] { 0.1 };

        assertThrows(IllegalArgumentException.class, () ->
                new QueryToken(null, iv, encryptedQry, plainQry, 1, 1, "ctx", 128, 0, 1)
        );
        assertThrows(IllegalArgumentException.class, () ->
                new QueryToken(List.of(), iv, encryptedQry, plainQry, 1, 1, "ctx", 128, 0, 1)
        );
        assertThrows(IllegalArgumentException.class, () ->
                new QueryToken(List.of(1), iv, encryptedQry, plainQry, -1, 1, "ctx", 128, 0, 1)
        );
        assertThrows(IllegalArgumentException.class, () ->
                new QueryToken(List.of(1), iv, encryptedQry, plainQry, 1, 0, "ctx", 128, 0, 1)
        );
    }


    @Test
    void gettersReturnClonesAndValues() {
        List<Integer> buckets     = List.of(2, 5, 7);
        byte[] iv                 = new byte[] { 0x42 };
        byte[] encryptedQry       = new byte[] { 0x13, 0x37 };
        double[] plainQry         = new double[] { 3.14, 2.71 };

        QueryToken tok = new QueryToken(
                buckets,
                iv,
                encryptedQry,
                plainQry,
                3,
                2,
                "epoch_1",
                128,
                0,
                2
        );

        assertEquals(buckets,             tok.getCandidateBuckets());
        assertArrayEquals(iv,             tok.getIv());
        assertArrayEquals(encryptedQry,   tok.getEncryptedQuery());
        assertArrayEquals(plainQry,       tok.getPlaintextQuery());
        assertEquals(3,                   tok.getTopK());
        assertEquals(2,                   tok.getNumTables());
        assertEquals("epoch_1",           tok.getEncryptionContext());
        assertEquals(128,                 tok.getDimension());
        assertEquals(0,                   tok.getShardId());
        assertEquals(2,                   tok.getVersion());

        // Mutation protection
        byte[] ivClone       = tok.getIv();
        byte[] qryClone      = tok.getEncryptedQuery();
        double[] plainClone  = tok.getPlaintextQuery();

        ivClone[0]      = 0x00;
        qryClone[0]     = 0x00;
        plainClone[1]   = 99.9;

        assertEquals(0x42,       tok.getIv()[0]);
        assertEquals(0x13,       tok.getEncryptedQuery()[0]);
        assertEquals(2.71,       tok.getPlaintextQuery()[1]);
    }
}
