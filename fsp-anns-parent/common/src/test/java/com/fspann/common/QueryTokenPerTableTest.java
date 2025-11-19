package com.fspann.common;

import org.junit.jupiter.api.Test;

import java.util.BitSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class QueryTokenPerTableTest {

    @Test
    void perTableConstructorClonesAndValidates() {
        List<List<Integer>> perTable = List.of(List.of(1, 2), List.of(3));

        BitSet[] codes = new BitSet[]{
                BitSet.valueOf(new byte[]{0b00000011}),
                BitSet.valueOf(new byte[]{0b00000101})
        };

        byte[] iv = new byte[12];
        byte[] enc = new byte[]{1, 2, 3};
        int numTables = perTable.size();
        int dimension = 32;

        QueryToken t = new QueryToken(
                perTable,
                codes,
                iv,
                enc,
                /* topK */ 5,
                numTables,
                "ctx",
                dimension,
                /* version */ 7
        );

        assertEquals(numTables, t.getNumTables());
        assertEquals(perTable, t.getTableBuckets());
        assertArrayEquals(iv, t.getIv());
        assertArrayEquals(enc, t.getEncryptedQuery());

        // codes must be cloned
        BitSet[] got = t.getCodes();
        assertNotSame(codes, got);
        assertEquals(codes[0], got[0]);
        assertEquals(codes[1], got[1]);

        // returned lists unmodifiable
        List<List<Integer>> tb = t.getTableBuckets();
        assertThrows(UnsupportedOperationException.class, () -> tb.add(List.of(9)));
    }

    @Test
    void rejectsEmptyInnerListAndBadIvLen() {
        BitSet[] codes = new BitSet[]{BitSet.valueOf(new byte[]{1})};

        // Empty inner list
        assertThrows(IllegalArgumentException.class, () ->
                new QueryToken(
                        List.of(List.of()),
                        codes,
                        new byte[12],
                        new byte[]{1},
                        1, 1, "ctx", 32, 1
                ));

        // Bad IV length
        assertThrows(IllegalArgumentException.class, () ->
                new QueryToken(
                        List.of(List.of(1)),
                        codes,
                        new byte[11],
                        new byte[]{1},
                        1, 1, "ctx", 32, 1
                ));
    }

    @Test
    void nullInnerListYieldsNpe() {
        BitSet[] codes = new BitSet[]{BitSet.valueOf(new byte[]{1})};

        assertThrows(NullPointerException.class, () ->
                new QueryToken(
                        java.util.Arrays.asList((List<Integer>) null),
                        codes,
                        new byte[12],
                        new byte[]{1},
                        1, 1, "ctx", 32, 1
                ));
    }
}
