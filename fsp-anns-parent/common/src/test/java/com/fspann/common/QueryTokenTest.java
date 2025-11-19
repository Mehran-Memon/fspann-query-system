package com.fspann.common;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class QueryTokenTest {

    @Test
    void constructorClonesAndAppliesDefaults() {
        // Outer list is copied (unmodifiable), inner lists are shared as-is.
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

        byte[] iv = new byte[12];           // must be 12 bytes
        byte[] enc = new byte[]{9, 8, 7};   // ciphertext
        int topK = 5;
        int numTables = 2;
        String ctx = "epoch_7_dim_128";
        int dim = 128;
        int version = 7;

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
        assertEquals(topK, token.getTopK());
        assertEquals(ctx, token.getEncryptionContext());
        assertEquals(dim, token.getDimension());
        assertEquals(version, token.getVersion());

        // Table buckets: outer list is copied & unmodifiable
        assertEquals(List.of(List.of(1, 2, 3), List.of(4, 5)), token.getTableBuckets());
        List<List<Integer>> gotBuckets = token.getTableBuckets();
        assertThrows(UnsupportedOperationException.class, () -> gotBuckets.add(List.of(9)));
        // Inner lists are *not* deep-copied; changing original inner list should not corrupt
        // the snapshot we already got from token.
        tableBuckets.get(0).set(0, 99);
        assertEquals(List.of(1, 2, 3), gotBuckets.get(0));

        // IV & ciphertext are cloned on input and on output
        byte[] ivFromToken = token.getIv();
        byte[] encFromToken = token.getEncryptedQuery();
        assertArrayEquals(iv, ivFromToken);
        assertArrayEquals(enc, encFromToken);

        // Mutating the original arrays does not affect token
        iv[0] = 1;
        enc[0] = 0;
        assertEquals(0, token.getIv()[0]);
        assertEquals(9, token.getEncryptedQuery()[0]);

        // Mutating returned arrays does not affect internal state
        ivFromToken[0] = 2;
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
        assertTrue(token.getCodes()[0].get(3));

        // Mutating returned BitSet array should not affect internal codes
        codesFromToken[0].clear(0);
        BitSet[] again = token.getCodes();
        assertTrue(again[0].get(0));
    }

    @Test
    void nullArgumentsAndIvLengthAreValidated() {
        BitSet[] codes = null;
        byte[] iv12 = new byte[12];
        byte[] enc = new byte[]{1};

        // tableBuckets null → NPE
        assertThrows(NullPointerException.class, () ->
                new QueryToken(null, codes, iv12, enc, 1, 1, "ctx", 2, 1));

        // iv null → NPE
        assertThrows(NullPointerException.class, () ->
                new QueryToken(List.of(List.of(1)), codes, null, enc, 1, 1, "ctx", 2, 1));

        // encryptedQuery null → NPE
        assertThrows(NullPointerException.class, () ->
                new QueryToken(List.of(List.of(1)), codes, iv12, null, 1, 1, "ctx", 2, 1));

        // IV must be 12 bytes
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
                null,       // encryptionContext null -> auto string
                0,          // invalid -> clamp to 1
                3
        );

        assertEquals(1, t1.getTopK(), "topK should be clamped to at least 1");
        assertEquals(1, t1.getNumTables(), "numTables should be clamped to at least 1");
        assertEquals(1, t1.getDimension(), "dimension should be clamped to at least 1");

        // When ctx is null, the default should be derived from version & dimension
        assertEquals("epoch_3_dim_1", t1.getEncryptionContext());
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
