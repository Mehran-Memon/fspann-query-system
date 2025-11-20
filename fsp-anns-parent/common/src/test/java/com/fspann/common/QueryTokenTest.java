// File: src/test/java/com/fspann/common/QueryTokenTest.java
package com.fspann.common;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class QueryTokenTest {

    @Test
    void constructorClonesAndAppliesDefaults() {
        // Outer list is copied (unmodifiable). In the current implementation
        // the inner lists are *shared* rather than deep-copied.
        List<List<Integer>> tableBuckets = new ArrayList<>();
        tableBuckets.add(new ArrayList<>(List.of(1, 2, 3)));
        tableBuckets.add(new ArrayList<>(List.of(4, 5)));

        // Codes: 2 divisions
        BitSet b0 = new BitSet();
        b0.set(0);
        b0.set(3);
        BitSet b1 = new BitSet();
        b1.set(1);
        BitSet[] codes = new BitSet[]{b0, b1};

        byte[] iv  = new byte[12];        // must be 12 bytes
        byte[] enc = new byte[]{9, 8, 7}; // ciphertext
        int topK      = 5;
        int numTables = 2;
        String ctx    = "epoch_7_dim_128";
        int dim       = 128;
        int version   = 7;

        QueryToken token = new QueryToken(
                tableBuckets,
                codes,
                iv,
                enc,
                topK,
                numTables,
                ctx,
                dim,
                version
        );

        // Basic fields
        assertEquals(numTables, token.getNumTables());
        assertEquals(topK,      token.getTopK());
        assertEquals(ctx,       token.getEncryptionContext());
        assertEquals(dim,       token.getDimension());
        assertEquals(version,   token.getVersion());

        // Table buckets: outer list is copied & unmodifiable
        List<List<Integer>> gotBuckets = token.getTableBuckets();
        assertEquals(List.of(List.of(1, 2, 3), List.of(4, 5)), gotBuckets);
        assertThrows(UnsupportedOperationException.class,
                () -> gotBuckets.add(List.of(9)),
                "Outer table bucket list should be unmodifiable");

        // In the current implementation, inner lists are shared (not deep-copied).
        // Mutating the original inner list will be visible in the token's view.
        tableBuckets.get(0).set(0, 99);
        assertEquals(List.of(99, 2, 3), gotBuckets.get(0));

        // IV & ciphertext are cloned on input and on output
        byte[] ivFromToken  = token.getIv();
        byte[] encFromToken = token.getEncryptedQuery();
        assertArrayEquals(iv,  ivFromToken);
        assertArrayEquals(enc, encFromToken);

        // Mutating the original arrays does not affect token
        iv[0]  = 1;
        enc[0] = 0;
        assertEquals(0, token.getIv()[0],            "IV should be defensive-copied");
        assertEquals(9, token.getEncryptedQuery()[0],"ciphertext should be defensive-copied");

        // Mutating returned arrays does not affect internal state
        ivFromToken[0]  = 2;
        encFromToken[0] = 3;
        assertEquals(0, token.getIv()[0]);
        assertEquals(9, token.getEncryptedQuery()[0]);

        // Codes: deep clone on getCodes()
        BitSet[] codesFromToken = token.getCodes();
        assertNotNull(codesFromToken);
        assertEquals(2, codesFromToken.length);
        assertEquals(b0, codesFromToken[0]);
        assertEquals(b1, codesFromToken[1]);

        // Mutating original BitSet should not affect token's copy
        b0.clear(3);
        assertTrue(token.getCodes()[0].get(3), "Codes should be deep-copied on construction");

        // Mutating returned BitSet array should not affect internal codes
        codesFromToken[0].clear(0);
        BitSet[] again = token.getCodes();
        assertTrue(again[0].get(0), "getCodes() should return a defensive copy");
    }

    @Test
    void nullArgumentsAndIvLengthAreValidated() {
        BitSet[] codes = null;
        byte[] iv12    = new byte[12];
        byte[] enc     = new byte[]{1};

        // tableBuckets null → NPE
        assertThrows(NullPointerException.class, () ->
                new QueryToken(null, codes, iv12, enc, 1, 1, "ctx", 2, 1));

        // iv null → NPE
        assertThrows(NullPointerException.class, () ->
                new QueryToken(List.of(List.of(1)), codes, null, enc, 1, 1, "ctx", 2, 1));

        // encryptedQuery null → NPE
        assertThrows(NullPointerException.class, () ->
                new QueryToken(List.of(List.of(1)), codes, iv12, null, 1, 1, "ctx", 2, 1));

        // IV must be exactly 12 bytes
        assertThrows(IllegalArgumentException.class, () ->
                new QueryToken(List.of(List.of(1)), codes, new byte[11], enc, 1, 1, "ctx", 2, 1));
        assertThrows(IllegalArgumentException.class, () ->
                new QueryToken(List.of(List.of(1)), codes, new byte[13], enc, 1, 1, "ctx", 2, 1));
    }

    @Test
    void topKNumTablesAndDimensionAreClampedNotRejected() {
        // topK <= 0 → clamped to 1
        QueryToken t1 = new QueryToken(
                List.of(List.of(1)),
                null,
                new byte[12],
                new byte[]{1},
                0,          // invalid -> clamp
                0,          // invalid -> clamp to 1
                null,       // encryptionContext null -> auto string from version & raw dim
                0,          // invalid -> dimension field clamped to 1, but context keeps raw dim
                3           // version
        );

        assertEquals(1, t1.getTopK(),      "topK should be clamped to at least 1");
        assertEquals(1, t1.getNumTables(), "numTables should be clamped to at least 1");
        assertEquals(1, t1.getDimension(), "dimension should be clamped to at least 1");

        // When ctx is null, the default is derived from version & *raw* dimension argument (0)
        assertEquals("epoch_3_dim_0", t1.getEncryptionContext());
    }

    @Test
    void getCodesReturnsNullWhenNotProvided() {
        QueryToken t = new QueryToken(
                List.of(List.of(1)),
                null,
                new byte[12],
                new byte[]{1},
                5,
                1,
                "ctx",
                2,
                1
        );
        assertNull(t.getCodes());
    }
}
