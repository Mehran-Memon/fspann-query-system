package com.fspann.api;

import com.fspann.common.Profiler;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class MicrometerProfiler extends Profiler {
    private static final Logger logger = LoggerFactory.getLogger(MicrometerProfiler.class);
    private final MeterRegistry registry;
    private final ConcurrentMap<String, Timer> timers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Long> startTimes = new ConcurrentHashMap<>();
    private final DistributionSummary clientSummary;
    private final DistributionSummary serverSummary;
    private final DistributionSummary ratioSummary;

    public MicrometerProfiler(MeterRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "MeterRegistry cannot be null");
        this.clientSummary = DistributionSummary.builder("fspann.query.client_ms").register(registry);
        this.serverSummary = DistributionSummary.builder("fspann.query.server_ms").register(registry);
        this.ratioSummary  = DistributionSummary.builder("fspann.query.ratio").register(registry);
    }

    @Override public void start(String op) {
        super.start(op);
        timers.computeIfAbsent(op, k -> Timer.builder("fspann.operation.duration").tag("operation", k).register(registry));
        startTimes.put(op, System.nanoTime());
    }

    @Override public void stop(String op) {
        super.stop(op);
        Timer t = timers.get(op);
        Long s = startTimes.remove(op);
        if (t != null && s != null) t.record(System.nanoTime() - s, TimeUnit.NANOSECONDS);
    }

    @Override public void recordQueryMetric(String id, double serverMs, double clientMs, double ratio) {
        super.recordQueryMetric(id, serverMs, clientMs, ratio);
        serverSummary.record(serverMs);
        clientSummary.record(clientMs);
        ratioSummary.record(ratio);
    }

    @Override
    public void exportToCSV(String filePath) {
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(filePath))) {
            writer.write("Label,AvgTime(ms),Runs\n");
            for (Map.Entry<String, Timer> entry : timers.entrySet()) {
                String label = entry.getKey();
                Timer timer = entry.getValue();
                long count = timer.count();
                double avgMs = count > 0 ? timer.totalTime(TimeUnit.MILLISECONDS) / count : 0.0;
                writer.write(String.format("%s,%.2f,%d%n", label, avgMs, count));
            }
            logger.info("Exported metrics to CSV: {}", filePath);
        } catch (IOException e) {
            logger.error("Failed to export metrics to CSV: {}", filePath, e);
        }
    }
}