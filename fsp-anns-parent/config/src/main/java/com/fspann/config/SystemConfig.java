package com.fspann.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;

/**
 * Application configuration loaded from a JSON or YAML file.
 * Provides shard and rotation settings, re-encryption batch size, and profiler toggle.
 */
public class SystemConfig {

    @JsonProperty("numShards")
    private int numShards = 32;

    @JsonProperty("opsThreshold")
    private long opsThreshold = 5_000_000;

    @JsonProperty("ageThresholdMs")
    private long ageThresholdMs = 7L * 24 * 60 * 60 * 1000; // 7 days

    @JsonProperty("reEncBatchSize")
    private int reEncBatchSize = 2_000;

    //    @JsonProperty("getCacheCapacity")
//    public static final int DEFAULT_CACHE_CAPACITY = 10_000;

    @JsonProperty("profilerEnabled")
    private boolean profilerEnabled = true;

    private SystemConfig() {
        // Jackson requires no-arg constructor
    }

    // Getters
    public int getNumShards() {
        return numShards;
    }

    public long getOpsThreshold() {
        return opsThreshold;
    }


    public long getAgeThresholdMs() {
        return ageThresholdMs;
    }

    public int getReEncBatchSize() {
        return reEncBatchSize;
    }

    public boolean isProfilerEnabled() {
        return profilerEnabled;
    }

    /**
     * Load configuration from a JSON or YAML file.
     * @param filePath path to the config file
     * @return SystemConfig instance populated with values
     * @throws IOException on failure to read or parse
     */
    public static SystemConfig load(String filePath) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        // Automatically detects JSON/YAML based on Jackson modules if added
        return mapper.readValue(new File(filePath), SystemConfig.class);
    }
}
