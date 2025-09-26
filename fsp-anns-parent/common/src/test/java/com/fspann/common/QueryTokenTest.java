package com.fspann.common;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class QueryTokenTest {
    @Test
    void constructorValidatesAndClones() {
        // two tables with their own bucket lists
        List<List<Integer>> tableBuckets = new ArrayList<>();
        tableBuckets.add(new ArrayList<>(List.of(1, 2, 3)));
        tableBuckets.add(new ArrayList<>(List.of(4, 5)));

        byte[] iv           = new byte[12];        // AES-GCM IV must be 12 bytes
        byte[] encryptedQry = new byte[] { 9, 8 };
        double[] plainQry   = new double[] { 1.0, 2.0 };

        QueryToken token = new QueryToken(
                tableBuckets,
                iv,
                encryptedQry,
                plainQry,
                4,          // topK
                2,          // numTables
                "ctx",
                128,        // dimension
                1           // version
        );

        assertEquals(2, token.getNumTables());
        assertEquals(4, token.getTopK());
        assertEquals("ctx", token.getEncryptionContext());
        assertEquals(128, token.getDimension());
        assertEquals(1, token.getVersion());

        // Values are preserved
        assertEquals(List.of(List.of(1, 2, 3), List.of(4, 5)), token.getTableBuckets());
        assertArrayEquals(iv,           token.getIv());
        assertArrayEquals(encryptedQry, token.getEncryptedQuery());
        assertArrayEquals(plainQry,     token.getPlaintextQuery());

        // Immutability checks (deep clone for buckets, clone for arrays)
        tableBuckets.get(0).set(0, 99);
        tableBuckets.add(List.of(7)); // mutate outer list too
        assertEquals(List.of(List.of(1, 2, 3), List.of(4, 5)), token.getTableBuckets());

        iv[0] = 7;
        assertNotEquals(iv[0], token.getIv()[0]);

        encryptedQry[1] = 5;
        assertNotEquals(encryptedQry[1], token.getEncryptedQuery()[1]);

        plainQry[0] = 99.9;
        assertNotEquals(plainQry[0], token.getPlaintextQuery()[0]);
    }

    @Test
    void constructorRejectsInvalidInputs() {
        byte[] iv12         = new byte[12];
        byte[] encryptedQry = new byte[] { 0x01 };
        double[] plainQry   = new double[] { 0.1 };

        // null tableBuckets → NPE
        assertThrows(NullPointerException.class, () ->
                new QueryToken(null, iv12, encryptedQry, plainQry, 1, 1, "ctx", 128, 1));

        // inner empty list → IAE
        assertThrows(IllegalArgumentException.class, () ->
                new QueryToken(List.of(List.of()), iv12, encryptedQry, plainQry, 1, 1, "ctx", 128, 1));

        // invalid topK
        assertThrows(IllegalArgumentException.class, () ->
                new QueryToken(List.of(List.of(1)), iv12, encryptedQry, plainQry, 0, 1, "ctx", 128, 1));
        assertThrows(IllegalArgumentException.class, () ->
                new QueryToken(List.of(List.of(1)), iv12, encryptedQry, plainQry, -1, 1, "ctx", 128, 1));

        // invalid numTables
        assertThrows(IllegalArgumentException.class, () ->
                new QueryToken(List.of(List.of(1)), iv12, encryptedQry, plainQry, 1, 0, "ctx", 128, 1));
        assertThrows(IllegalArgumentException.class, () ->
                new QueryToken(List.of(List.of(1)), iv12, encryptedQry, plainQry, 1, -3, "ctx", 128, 1));

        // invalid dimension
        assertThrows(IllegalArgumentException.class, () ->
                new QueryToken(List.of(List.of(1)), iv12, encryptedQry, plainQry, 1, 1, "ctx", 0, 1));
        assertThrows(IllegalArgumentException.class, () ->
                new QueryToken(List.of(List.of(1)), iv12, encryptedQry, plainQry, 1, 1, "ctx", -5, 1));

        // invalid IV length (must be 12 bytes)
        assertThrows(IllegalArgumentException.class, () ->
                new QueryToken(List.of(List.of(1)), new byte[11], encryptedQry, plainQry, 1, 1, "ctx", 128, 1));
        assertThrows(IllegalArgumentException.class, () ->
                new QueryToken(List.of(List.of(1)), new byte[13], encryptedQry, plainQry, 1, 1, "ctx", 128, 1));
    }

    @Test
    void gettersReturnClonesAndValues() {
        List<List<Integer>> buckets = List.of(List.of(2, 5, 7));
        byte[] iv                 = new byte[12];
        byte[] encryptedQry       = new byte[] { 0x13, 0x37 };
        double[] plainQry         = new double[] { 3.14, 2.71 };

        QueryToken tok = new QueryToken(
                buckets,
                iv,
                encryptedQry,
                plainQry,
                3,
                1,
                "epoch_1",
                128,
                2
        );

        assertEquals(buckets,            tok.getTableBuckets());
        assertArrayEquals(iv,            tok.getIv());
        assertArrayEquals(encryptedQry,  tok.getEncryptedQuery());
        assertArrayEquals(plainQry,      tok.getPlaintextQuery());
        assertEquals(3,                  tok.getTopK());
        assertEquals(1,                  tok.getNumTables());
        assertEquals("epoch_1",          tok.getEncryptionContext());
        assertEquals(128,                tok.getDimension());
        assertEquals(2,                  tok.getVersion());

        // Mutation protection on clones
        byte[] ivClone      = tok.getIv();
        byte[] qryClone     = tok.getEncryptedQuery();
        double[] plainClone = tok.getPlaintextQuery();

        ivClone[0]    = 0x00;
        qryClone[0]   = 0x00;
        plainClone[1] = 99.9;

        assertEquals(0,    tok.getIv()[0]);
        assertEquals(0x13, tok.getEncryptedQuery()[0]);
        assertEquals(2.71, tok.getPlaintextQuery()[1]);
    }
}
