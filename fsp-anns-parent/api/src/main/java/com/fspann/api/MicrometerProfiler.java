package com.fspann.api;

import com.fspann.common.Profiler;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public final class MicrometerProfiler {

    private static final Logger log =
            LoggerFactory.getLogger(MicrometerProfiler.class);

    private final Profiler base;
    private final MeterRegistry registry;

    private final Map<String, Timer> timers = new HashMap<>();
    private final Map<String, Long> startTimes = new HashMap<>();

    private final DistributionSummary clientSummary;
    private final DistributionSummary serverSummary;
    private final DistributionSummary distanceRatioSummary;      // ← Paper ratio
    private final DistributionSummary candidateRatioSummary;     // ← Search efficiency

    public MicrometerProfiler(MeterRegistry reg, Profiler baseProfiler) {
        this.registry = Objects.requireNonNull(reg, "registry");
        this.base = Objects.requireNonNull(baseProfiler, "baseProfiler");

        this.clientSummary =
                DistributionSummary.builder("fspann.query.client_ms")
                        .register(registry);
        this.serverSummary =
                DistributionSummary.builder("fspann.query.server_ms")
                        .register(registry);
        this.distanceRatioSummary =
                DistributionSummary.builder("fspann.query.distance_ratio")
                        .description("Paper ratio (quality metric)")
                        .register(registry);
        this.candidateRatioSummary =
                DistributionSummary.builder("fspann.query.candidate_ratio")
                        .description("Search efficiency metric")
                        .register(registry);
    }

    /* --------------------------------------------------------
     * TIMING WRAPPER
     * -------------------------------------------------------- */

    public synchronized void start(String op) {
        base.start(op);

        timers.computeIfAbsent(
                op,
                k -> Timer.builder("fspann.operation.duration")
                        .tag("op", k)
                        .register(registry)
        );

        startTimes.put(op, System.nanoTime());
    }

    public synchronized void stop(String op) {
        base.stop(op);

        Long st = startTimes.remove(op);
        Timer t = timers.get(op);
        if (t != null && st != null) {
            t.record(System.nanoTime() - st, TimeUnit.NANOSECONDS);
        }
    }

    /* --------------------------------------------------------
     * QUERY ROW WRAPPER (UPDATED: both ratios)
     * -------------------------------------------------------- */

    public synchronized void recordQueryRow(
            String label,
            double serverMs,
            double clientMs,
            double runMs,
            double decryptMs,
            double insertMs,
            double distanceRatio,          // ← Paper ratio (quality)
            double candidateRatio,         // ← Search efficiency
            double recall,
            int candTotal,
            int candKept,
            int candDec,
            int returned,
            int tokenBytes,
            int vectorDim,
            int tokenK,
            int tokenKBase,
            int qIndex,
            int totalFlushed,
            int flushThreshold,
            int touchedCount,
            int reencCount,
            long reencMs,
            long reencDelta,
            long reencAfter,
            String ratioDenomSource,
            String mode,
            int stableRaw,
            int stableFinal,
            int nnRank,
            boolean nnSeen
    ) {
        base.recordQueryRow(
                label,
                serverMs,
                clientMs,
                runMs,
                decryptMs,
                insertMs,
                distanceRatio,         // ← Pass both ratios
                candidateRatio,
                recall,
                candTotal,
                candKept,
                candDec,
                returned,
                tokenBytes,
                vectorDim,
                tokenK,
                tokenKBase,
                qIndex,
                totalFlushed,
                flushThreshold,
                touchedCount,
                reencCount,
                reencMs,
                reencDelta,
                reencAfter,
                ratioDenomSource,
                mode,
                stableRaw,
                stableFinal,
                nnRank,
                nnSeen
        );

        clientSummary.record(clientMs);
        serverSummary.record(serverMs);

        // Record both ratio metrics
        if (Double.isFinite(distanceRatio)) {
            distanceRatioSummary.record(distanceRatio);
        }
        if (Double.isFinite(candidateRatio)) {
            candidateRatioSummary.record(candidateRatio);
        }
    }

    /* --------------------------------------------------------
     * EXPORTS
     * -------------------------------------------------------- */

    public void exportMetersCSV(String path) {
        StringBuilder sb =
                new StringBuilder("name,tags,count,totalMs,meanMs,maxMs\n");

        for (Meter m : registry.getMeters()) {
            if (m instanceof Timer t) {
                long count = t.count();
                double total = t.totalTime(TimeUnit.MILLISECONDS);
                double mean = (count == 0 ? 0.0 : total / count);
                double max = t.max(TimeUnit.MILLISECONDS);

                sb.append(t.getId().getName()).append(',')
                        .append(t.getId().getTags()).append(',')
                        .append(count).append(',')
                        .append(String.format(Locale.ROOT, "%.3f", total)).append(',')
                        .append(String.format(Locale.ROOT, "%.3f", mean)).append(',')
                        .append(String.format(Locale.ROOT, "%.3f", max)).append('\n');
            }
        }

        try {
            Files.createDirectories(Paths.get(path).getParent());
            Files.writeString(Paths.get(path), sb.toString());
        } catch (IOException e) {
            log.warn("Failed to export Micrometer CSV to {}", path);
        }
    }

    public Profiler getBase() {
        return base;
    }
}