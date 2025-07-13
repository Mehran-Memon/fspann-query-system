package com.fspann.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Application configuration loaded from a JSON or YAML file.
 * Provides shard and rotation settings, re-encryption batch size, and profiler toggle.
 */
public class SystemConfig {
    private static final Logger logger = LoggerFactory.getLogger(SystemConfig.class);

    @JsonProperty("numShards")
    private int numShards = 32;

    @JsonProperty("opsThreshold")
    private long opsThreshold = 500_000_000;

    @JsonProperty("ageThresholdMs")
    private long ageThresholdMs = 7L * 24 * 60 * 60 * 1000; // 7 days

    @JsonProperty("reEncBatchSize")
    private int reEncBatchSize = 2_000;

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

    public void validate() throws ConfigLoadException {
        if (numShards <= 0) throw new ConfigLoadException("numShards must be positive", null);
        if (opsThreshold <= 0) throw new ConfigLoadException("opsThreshold must be positive", null);
        if (ageThresholdMs <= 0) throw new ConfigLoadException("ageThresholdMs must be positive", null);
        if (reEncBatchSize <= 0) throw new ConfigLoadException("reEncBatchSize must be positive", null);
    }

    /**
     * Load configuration from a JSON or YAML file.
     * @param filePath path to the config file
     * @return SystemConfig instance populated with values
     * @throws ConfigLoadException on failure to read, parse, or validate
     */
    public static SystemConfig load(String filePath) throws ConfigLoadException {
        ObjectMapper mapper = new ObjectMapper();
        try {
            SystemConfig config = mapper.readValue(new File(filePath), SystemConfig.class);
            config.validate();
//            logger.info("Successfully loaded config from {}", filePath);
            return config;
        } catch (IOException e) {
//            logger.error("Failed to load config from {}", filePath, e);
            throw new ConfigLoadException("Unable to load configuration: " + e.getMessage(), e);
        }
    }

    public static class ConfigLoadException extends Exception {
        public ConfigLoadException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}