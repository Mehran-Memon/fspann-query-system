package com.fspann.common;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assumptions;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class QueryTokenLegacyBridgeTest {

    @Test
    void legacyReplicatesAcrossTables() throws Exception {
        // Legacy ctor signature we expect:
        // (List<Integer>, byte[], byte[], double[], int, int, String, int, int, int)
        Class<?>[] LEGACY_SIG = new Class<?>[]{
                List.class, byte[].class, byte[].class, double[].class,
                int.class, int.class, String.class, int.class, int.class, int.class
        };

        // If the legacy constructor isn't present in this build, skip the test.
        boolean hasLegacy =
                Arrays.stream(QueryToken.class.getDeclaredConstructors())
                        .anyMatch(c -> Arrays.equals(c.getParameterTypes(), LEGACY_SIG));
        Assumptions.assumeTrue(hasLegacy, "Legacy QueryToken constructor not present; skipping legacy bridge test.");

        // Bind to the legacy constructor explicitly
        Constructor<QueryToken> legacyCtor = QueryToken.class.getDeclaredConstructor(LEGACY_SIG);

        List<Integer> candidateBuckets = List.of(7, 11);

        QueryToken legacy = legacyCtor.newInstance(
                candidateBuckets,
                new byte[12],        // be generous: 12B IV works across variants
                new byte[]{2},
                new double[]{0.5},
                5,                   // topK
                3,                   // numTables (L)
                "ctx",
                32,                  // dimension
                0,                   // shardId (ignored on new path)
                1                    // version
        );

        // Legacy tokens shouldn’t report per-table buckets
        assertFalse(legacy.hasPerTable(), "Legacy token should report no per-table buckets");
        assertEquals(3, legacy.getNumTables(), "Legacy token should retain numTables");

        // Bridge should replicate the legacy buckets across L tables
        var per = legacy.getTableBucketsOrLegacy();
        assertEquals(3, per.size(), "Should replicate across L tables");
        assertEquals(List.of(7, 11), per.get(0));
        assertEquals(List.of(7, 11), per.get(1));
        assertEquals(List.of(7, 11), per.get(2));
    }
}
