package com.fspann.query;

import com.fspann.common.Profiler;
import com.fspann.query.core.Aggregates;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Search Pipeline Integration Tests (31-arg QueryRow)")
class SearchPipelineIntegrationTest {

    private static final double EPSILON = 1e-9;

    @Test
    void testMultiKRecordingWithRecall() {
        Profiler p = new Profiler();

        int[] Ks = {1, 10, 100};

        for (int k : Ks) {
            double precision = 0.25;
            double recall = precision;

            p.recordQueryRow(
                    "Q_K" + k,
                    10.0, 10.0, 20.0, 5.0, 0.0,
                    1.0 + 0.01 * k,
                    recall,

                    1000, 200, 200, k,
                    128, 128,
                    k, k,
                    0,
                    0, 0,
                    0,
                    0,
                    0L, 0L, 0L,
                    "gt",
                    "partitioned",
                    1000,
                    200,
                    -1,
                    false
            );
        }

        Aggregates agg = Aggregates.fromProfiler(p);

        for (int k : Ks) {
            assertTrue(agg.precisionAtK.containsKey(k));
            assertTrue(agg.recallAtK.containsKey(k));
            assertEquals(
                    agg.precisionAtK.get(k),
                    agg.recallAtK.get(k),
                    EPSILON
            );
        }
    }

    @Test
    void testReencryptionAccountingWithRecall() {
        Profiler p = new Profiler();

        double precision = 0.3;
        double recall = precision;

        p.recordQueryRow(
                "Q",
                10.0, 10.0, 20.0, 5.0, 0.0,
                1.2,
                recall,

                100, 50, 50, 10,
                128, 128,
                10, 10,
                0,
                0, 0,
                10,
                5,
                100L, 200L, 1000L,
                "gt",
                "partitioned",
                100,
                50,
                -1,
                false
        );

        Aggregates agg = Aggregates.fromProfiler(p);

        assertEquals(5, agg.reencryptCount);
        assertEquals(200L, agg.reencryptBytes);
        assertEquals(100.0, agg.reencryptMs, EPSILON);
    }
}
