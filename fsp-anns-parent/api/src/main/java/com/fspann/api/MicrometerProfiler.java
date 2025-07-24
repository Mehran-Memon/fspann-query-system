package com.fspann.api;

import com.fspann.common.Profiler;
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

    public MicrometerProfiler(MeterRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "MeterRegistry cannot be null");
    }

    @Override
    public void start(String operation) {
        Timer timer = timers.computeIfAbsent(operation,
                k -> Timer.builder("fspann.operation.duration")
                        .tag("operation", k)
                        .register(registry));
        startTimes.put(operation, System.nanoTime());
        logger.debug("Started profiling operation: {}", operation);
    }

    @Override
    public void stop(String operation) {
        Timer timer = timers.get(operation);
        Long startTime = startTimes.remove(operation);
        if (timer != null && startTime != null) {
            long durationNs = System.nanoTime() - startTime;
            timer.record(durationNs, TimeUnit.NANOSECONDS);
            logger.debug("Stopped profiling operation: {}, duration: {} ns", operation, durationNs);
        } else {
            logger.warn("No timer or start time found for operation: {}", operation);
        }
    }

    @Override
    public void recordQueryMetric(String queryId, double serverMs, double clientMs, double ratio) {
        registry.gauge("fspann.query.server_ms", serverMs);
        registry.gauge("fspann.query.client_ms", clientMs);
        registry.gauge("fspann.query.ratio", ratio);
        logger.debug("Recorded query metrics for {}: serverMs={}, clientMs={}, ratio={}",
                queryId, serverMs, clientMs, ratio);
    }

    @Override
    public void exportToCSV(String filePath) {
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(filePath))) {
            writer.write("Operation,DurationNs\n");
            for (Map.Entry<String, Timer> entry : timers.entrySet()) {
                writer.write(String.format("%s,%d\n", entry.getKey(), entry.getValue().totalTime(TimeUnit.NANOSECONDS)));
            }
            logger.info("Exported metrics to CSV: {}", filePath);
        } catch (IOException e) {
            logger.error("Failed to export metrics to CSV: {}", filePath, e);
        }
    }
}