package com.fspann.common;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class QueryTokenTest {

    @Test
    void tokenStoresFieldsCorrectly() {
        QueryToken t = new QueryToken(
                Collections.emptyList(),
                null,
                new byte[12],
                new byte[16],
                10,
                0,
                "ctx",
                128,
                1
        );

        assertEquals(10, t.getTopK());
        assertEquals(128, t.getDimension());
        assertEquals("ctx", t.getEncryptionContext());
    }
}
