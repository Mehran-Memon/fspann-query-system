package com.fspann.common;

import org.junit.jupiter.api.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class QueryTokenTest {

    @Test
    void testFields() {
        QueryToken t = new QueryToken(
                Collections.emptyList(),
                new BitSet[]{new BitSet()},
                new byte[12],
                new byte[16],
                5,
                0,
                "ctx",
                32,
                3
        );

        assertEquals(5, t.getTopK());
        assertEquals(32, t.getDimension());
        assertEquals("ctx", t.getEncryptionContext());
    }

    @Test
    void testCloneCodes() {
        BitSet bs = new BitSet();
        bs.set(1);
        QueryToken t = new QueryToken(
                List.of(),
                new BitSet[]{bs},
                new byte[12],
                new byte[16],
                5,
                0,
                "ctx",
                16,
                1
        );

        BitSet[] c1 = t.getCodes();
        BitSet[] c2 = t.getCodes();
        assertNotSame(c1, c2);
    }
}
