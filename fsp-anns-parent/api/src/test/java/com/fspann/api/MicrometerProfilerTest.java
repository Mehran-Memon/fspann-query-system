package com.fspann.api;

import com.fspann.common.Profiler;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.util.List;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class MicrometerProfilerTest {

    private MeterRegistry registry;
    private Profiler base;
    private MicrometerProfiler prof;

    @BeforeEach
    void setup() {
        registry = new SimpleMeterRegistry();
        base = mock(Profiler.class);
        prof = new MicrometerProfiler(registry, base);
    }

    /* ------------------------------------------------------------
     * 1. start()/stop() registers a Timer and records duration
     * ------------------------------------------------------------ */
    @Test
    void startStop_recordsTimer() throws Exception {
        prof.start("opA");
        Thread.sleep(30);
        prof.stop("opA");

        Timer timer = registry.find("fspann.operation.duration").timer();
        assertNotNull(timer, "Timer must exist after start/stop");

        assertEquals(1, timer.count(), "Exactly 1 recording expected");
        assertTrue(timer.totalTime(TimeUnit.MILLISECONDS) >= 20,
                "Duration should be >= 20ms");
    }

    /* ------------------------------------------------------------
     * 2. recordQueryRow updates DistributionSummaries
     * ------------------------------------------------------------ */
    @Test
    void recordQueryRow_updatesSummaries() {
        prof.recordQueryRow(
                "Q0",
                12.0, 6.0, 18.0, 2.0, 1.0,
                1.23, 0.50,
                10, 9, 8, 5,
                32, 128,
                100, 100,
                0,
                0, 0,
                0, 0,
                0L, 0L, 0L,
                "gt",
                "full", 0, 0
        );

        DistributionSummary client = registry.find("fspann.query.client_ms").summary();
        DistributionSummary server = registry.find("fspann.query.server_ms").summary();
        DistributionSummary ratio  = registry.find("fspann.query.ratio").summary();

        assertNotNull(client);
        assertNotNull(server);
        assertNotNull(ratio);

        assertEquals(1, client.count());
        assertEquals(1, server.count());
        assertEquals(1, ratio.count());

        assertEquals(6.0, client.totalAmount(), 1e-6);
        assertEquals(12.0, server.totalAmount(), 1e-6);
        assertEquals(1.23, ratio.totalAmount(), 1e-6);
    }

    /* ------------------------------------------------------------
     * 3. recordQueryRow delegates to base Profiler
     * ------------------------------------------------------------ */
    @Test
    void recordQueryRow_delegatesToBaseProfiler() {
        prof.recordQueryRow(
                "Q0",
                12.0, 6.0, 18.0, 2.0, 1.0,
                1.23, 0.50,
                10, 9, 8, 5,
                32, 128,
                100, 100,
                0,
                0, 0,
                0, 0,
                0L, 0L, 0L,
                "gt",
                "full", 0, 0
        );

        verify(base, times(1)).recordQueryRow(
                "Q0",
                12.0, 6.0, 18.0, 2.0, 1.0,
                1.23, 0.50,
                10, 9, 8, 5,
                32, 128,
                100, 100,
                0,
                0, 0,
                0, 0,
                0L, 0L, 0L,
                "gt",
                "full", 0 ,0
        );

    }

    /* ------------------------------------------------------------
     * 4. exportMetersCSV writes all timers to a CSV file
     * ------------------------------------------------------------ */
    @Test
    void exportMetersCSV_writesExpectedCsv(@TempDir Path tmp) throws Exception {
        // produce some meter activity
        prof.start("TEST_OP");
        Thread.sleep(20);
        prof.stop("TEST_OP");

        Path out = tmp.resolve("meters.csv");
        prof.exportMetersCSV(out.toString());

        assertTrue(Files.exists(out), "CSV must exist");

        List<String> lines = Files.readAllLines(out);
        assertFalse(lines.isEmpty(), "CSV must not be empty");

        // CSV header
        assertEquals("name,tags,count,totalMs,meanMs,maxMs", lines.get(0));

        // One timer row expected
        boolean found = lines.stream().anyMatch(s -> s.contains("fspann.operation.duration"));
        assertTrue(found, "CSV should contain our timer");
    }
}
