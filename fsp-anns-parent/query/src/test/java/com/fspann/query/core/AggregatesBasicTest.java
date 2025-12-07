package com.fspann.query.core;

import com.fspann.common.Profiler;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class AggregatesBasicTest {

    @Test
    void aggregatesComputeMeansCorrectly() {
        Profiler p = new Profiler();

        p.recordQueryRow(
                "q1",
                10, 20, 30,      // server, client, runMs
                5, 1,            // decrypt, insert
                0.5,             // ratio
                0.9,             // precision
                100, 50, 20, 10, // cand total/kept/decrypted/returned
                512, 128,        // tokenBytes, vectorDim
                10, 10,          // tokenK, tokenKBase
                0,               // qIndex
                0, 0,            // flushed, threshold
                0,               // touchedCount
                0,               // reencCount
                0, 0, 0,         // reencTimeMs, reencDelta, reencAfter
                "base",
                "full"           // mode MUST be "full" or precision@K is ignored
        );

        p.recordQueryRow(
                "q2",
                30, 10, 50,
                8, 1,
                0.7,
                0.8,
                200, 80, 40, 20,
                512, 128,
                10, 10,
                1,
                0, 0,
                0,
                0,
                0, 0, 0,
                "base",
                "full"
        );

        Aggregates ag = Aggregates.fromProfiler(p);

        assertEquals(0.6, ag.avgRatio, 1e-6);
        assertEquals(20, ag.avgServerMs, 1e-6);
        assertEquals(15, ag.avgClientMs, 1e-6);

        // precisionAtK only populated when mode = "full"
        assertEquals(0.85, ag.precisionAtK.get(10), 1e-6);
    }
}
