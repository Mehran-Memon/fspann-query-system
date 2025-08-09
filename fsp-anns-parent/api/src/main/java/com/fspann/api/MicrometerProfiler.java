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
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import io.micrometer.core.instrument.*;

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

    @Override
    public void start(String op) {
        super.start(op);
        timers.computeIfAbsent(op, k -> Timer.builder("fspann.operation.duration").tag("operation", k).register(registry));
        startTimes.put(op, System.nanoTime());
    }

    @Override
    public void stop(String op) {
        super.stop(op);
        Timer t = timers.get(op);
        Long s = startTimes.remove(op);
        if (t != null && s != null) t.record(System.nanoTime() - s, TimeUnit.NANOSECONDS);
    }

        public void exportMetersCSV(String path) {
            StringBuilder sb = new StringBuilder("name,tags,count,totalMs,meanMs,maxMs\n");
            for (Meter m : registry.getMeters()) {
                Meter.Id id = m.getId();
                if (m instanceof Timer t) {
                    long count = t.count();
                    double totalMs = t.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS);
                    double meanMs = (count == 0) ? 0 : totalMs / count;
                    double maxMs = t.max(java.util.concurrent.TimeUnit.MILLISECONDS);
                    sb.append(id.getName()).append(',')
                            .append(id.getTags()).append(',')
                            .append(count).append(',')
                            .append(String.format(java.util.Locale.ROOT,"%.3f", totalMs)).append(',')
                            .append(String.format(java.util.Locale.ROOT,"%.3f", meanMs)).append(',')
                            .append(String.format(java.util.Locale.ROOT,"%.3f", maxMs)).append('\n');
                }
                if (m instanceof Counter c) {
                    sb.append(id.getName()).append(',')
                            .append(id.getTags()).append(',')
                            .append((long)c.count()).append(",0,0,0\n");
                }
            }
            try {
                java.nio.file.Files.createDirectories(java.nio.file.Paths.get(path).getParent());
                java.nio.file.Files.writeString(java.nio.file.Paths.get(path), sb.toString());
            } catch (Exception e) {
                // log/warn
            }
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
    private static final class QRow {
        final String id; final double server; final double client; final double ratio;
        QRow(String id, double s, double c, double r){ this.id=id; this.server=s; this.client=c; this.ratio=r; }
    }

    private final java.util.List<QRow> queryRows =
            Collections.synchronizedList(new java.util.ArrayList<>(1024));

    @Override
    public void recordQueryMetric(String queryId, double serverMs, double clientMs, double ratio) {
        super.recordQueryMetric(queryId, serverMs, clientMs, ratio);
        queryRows.add(new QRow(queryId, serverMs, clientMs, ratio));
    }

    public void exportQueryMetricsCSV(String filePath) {
        try (var w = Files.newBufferedWriter(Paths.get(filePath))) {
            w.write("QueryId,ServerMs,ClientMs,Ratio\n");
            synchronized (queryRows) {
                for (QRow r : queryRows) {
                    w.write(String.format("%s,%.3f,%.3f,%.6f%n", r.id, r.server, r.client, r.ratio));
                }
            }
        } catch (IOException e) {
            logger.error("Failed to export query metrics CSV: {}", filePath, e);
        }
    }
}