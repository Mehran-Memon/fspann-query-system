package com.fspann.query;

import com.fspann.common.QueryMetrics;
import com.fspann.common.Profiler;
import com.fspann.query.core.Aggregates;
import org.junit.jupiter.api.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Metrics and Evaluation Semantics Tests (31-arg QueryRow)")
class MetricsAndSearchTest {

    private static final double EPSILON = 1e-9;

    @Test
    void testAggregatesWithRecall() {
        Profiler p = new Profiler();

        double ratio = 1.2;
        double precision = 0.4;
        double recall = precision; // PPANN invariant

        p.recordQueryRow(
                "Q0_K10",
                10.0, 20.0, 30.0, 5.0, 0.0,
                ratio,
                recall,

                100, 50, 50, 10,
                128, 128,
                10, 10,
                0,
                0, 0,
                0,
                0,
                0L, 0L, 0L,
                "gt",
                "partitioned",
                100,
                50,
                -1,
                false
        );

        Aggregates agg = Aggregates.fromProfiler(p);

        assertEquals(ratio, agg.avgCandidateRatio, EPSILON);
        assertEquals(recall, agg.avgRecall, EPSILON);

        assertTrue(agg.precisionAtK.containsKey(10));
        assertEquals(precision, agg.precisionAtK.get(10), EPSILON);
    }

}