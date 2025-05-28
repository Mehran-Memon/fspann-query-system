package com.fspann.api;

import com.fspann.common.Profiler;
import io.micrometer.prometheus.PrometheusMeterRegistry;

public class MicrometerProfiler extends Profiler {
    public MicrometerProfiler(PrometheusMeterRegistry registry) {
        super();
    }
}
