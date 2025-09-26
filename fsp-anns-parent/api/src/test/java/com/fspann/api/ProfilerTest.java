package com.fspann.api;

import com.fspann.common.Profiler;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ProfilerTest {
    @Test
    void testTimingMeasurement() {
        Profiler profiler = new Profiler();
        profiler.start("testOp");
        // Simulate some work
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        profiler.stop("testOp");
        List<Long> timings = profiler.getTimings("testOp");
        assertFalse(timings.isEmpty(), "Timings list should not be empty");
        assertTrue(timings.get(0) >= 80_000_000L, ">= ~80ms in ns");
    }

    @Test
    void testMultipleTimings() {
        Profiler profiler = new Profiler();
        for (int i = 0; i < 3; i++) {
            profiler.start("multiOp");
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            profiler.stop("multiOp");
        }
        List<Long> timings = profiler.getTimings("multiOp");
        assertEquals(3, timings.size(), "Should have 3 timing entries");
        for (Long timing : timings) {
            assertTrue(timing >= 40_000_000L, ">= ~40ms in ns");
        }
    }
}