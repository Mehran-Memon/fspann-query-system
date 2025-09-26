package com.fspann.common;

import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class QueryTokenLegacyBridgeTest {
    @Test
    void legacyReplicatesAcrossTables() {
        QueryToken legacy = new QueryToken(
                List.of(7, 11),
                new byte[]{1},
                new byte[]{2},
                new double[]{0.5},
                5, 3, "ctx", 32, 0, 1
        );
        assertFalse(legacy.hasPerTable());
        var per = legacy.getTableBucketsOrLegacy(3);
        assertEquals(3, per.size());
        assertEquals(List.of(7,11), per.get(0));
        assertEquals(List.of(7,11), per.get(1));
        assertEquals(List.of(7,11), per.get(2));
    }
}
