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
        try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        profiler.stop("testOp");
        List<Long> timings = profiler.getTimings("testOp");
        assertFalse(timings.isEmpty());
        assertTrue(timings.get(0) >= 100_000_000); // At least 100ms in nanoseconds
    }
}
