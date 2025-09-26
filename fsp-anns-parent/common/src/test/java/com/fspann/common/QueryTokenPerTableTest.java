package com.fspann.common;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class QueryTokenPerTableTest {

    @Test
    void perTableConstructorClonesAndValidates() {
        List<List<Integer>> perTable = List.of(List.of(1, 2), List.of(3));
        byte[] iv = new byte[12];                    // AES-GCM IV must be 12 bytes
        byte[] enc = new byte[]{1, 2, 3};
        double[] q = new double[]{0.1, 0.2};         // 2D query
        int numTables = perTable.size();             // keep equal
        int dimension = q.length;                    // must match

        QueryToken t = new QueryToken(
                perTable, iv, enc, q,
                5, numTables, "ctx", dimension, 7
        );

        assertEquals(numTables, t.getNumTables());
        assertEquals(perTable, t.getTableBuckets());
        assertArrayEquals(iv, t.getIv());

        // Returned list is unmodifiable
        List<List<Integer>> got = t.getTableBuckets();
        assertThrows(UnsupportedOperationException.class, () -> got.add(List.of(9)));
        assertThrows(UnsupportedOperationException.class, () -> got.get(0).add(99));
        assertEquals(List.of(List.of(1, 2), List.of(3)), t.getTableBuckets());
    }

    @Test
    void rejectsEmptyInnerListAndBadIvLen() {
        // Empty inner list → IAE
        assertThrows(IllegalArgumentException.class, () ->
                new QueryToken(List.of(List.of()), new byte[12], new byte[]{1}, new double[]{0.1}, 1, 1, "ctx", 1, 1));

        // Bad IV length → IAE
        assertThrows(IllegalArgumentException.class, () ->
                new QueryToken(List.of(List.of(1)), new byte[11], new byte[]{1}, new double[]{0.1}, 1, 1, "ctx", 1, 1));
    }

    @Test
    void nullInnerListYieldsNpe() {
        assertThrows(NullPointerException.class, () ->
                new QueryToken(java.util.Arrays.asList((List<Integer>) null), new byte[12], new byte[]{1}, new double[]{0.1}, 1, 1, "ctx", 1, 1));
    }
}
