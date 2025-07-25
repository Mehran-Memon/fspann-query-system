package com.fspann.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Application configuration loaded from a JSON or YAML file.
 * Provides shard and rotation settings, re-encryption batch size, and profiler toggle.
 */
public class SystemConfig {
    private static final Logger logger = LoggerFactory.getLogger(SystemConfig.class);
    private static final int MAX_SHARDS = 1024;
    private static final long MAX_OPS_THRESHOLD = 1_000_000_000L;
    private static final long MAX_AGE_THRESHOLD_MS = 30L * 24 * 60 * 60 * 1000; // 30 days
    private static final int MAX_REENC_BATCH_SIZE = 10_000;
    private static final ConcurrentMap<String, SystemConfig> configCache = new ConcurrentHashMap<>();

    @JsonProperty("numShards")
    private int numShards = 32;

    @JsonProperty("opsThreshold")
    private long opsThreshold = 500_000_000L;

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
        if (numShards <= 0) {
            throw new ConfigLoadException("numShards must be positive", null);
        }
        if (numShards > MAX_SHARDS) {
            logger.warn("numShards {} exceeds maximum {}, capping", numShards, MAX_SHARDS);
            numShards = MAX_SHARDS;
        }
        if (opsThreshold <= 0) {
            throw new ConfigLoadException("opsThreshold must be positive", null);
        }
        if (opsThreshold > MAX_OPS_THRESHOLD) {
            logger.warn("opsThreshold {} exceeds maximum {}, capping", opsThreshold, MAX_OPS_THRESHOLD);
            opsThreshold = MAX_OPS_THRESHOLD;
        }
        if (ageThresholdMs <= 0) {
            throw new ConfigLoadException("ageThresholdMs must be positive", null);
        }
        if (ageThresholdMs > MAX_AGE_THRESHOLD_MS) {
            logger.warn("ageThresholdMs {} exceeds maximum {}, capping", ageThresholdMs, MAX_AGE_THRESHOLD_MS);
            ageThresholdMs = MAX_AGE_THRESHOLD_MS;
        }
        if (reEncBatchSize <= 0) {
            throw new ConfigLoadException("reEncBatchSize must be positive", null);
        }
        if (reEncBatchSize > MAX_REENC_BATCH_SIZE) {
            logger.warn("reEncBatchSize {} exceeds maximum {}, capping", reEncBatchSize, MAX_REENC_BATCH_SIZE);
            reEncBatchSize = MAX_REENC_BATCH_SIZE;
        }
    }

    /**
     * Load configuration from a JSON or YAML file.
     * @param filePath path to the config file
     * @return SystemConfig instance populated with values
     * @throws ConfigLoadException on failure to read, parse, or validate
     */
    public static SystemConfig load(String filePath) throws ConfigLoadException {
        Objects.requireNonNull(filePath, "Config file path cannot be null");
        Path path = Paths.get(filePath).normalize();
        // For testability, allow any absolute path:
        if (!Files.isReadable(path)) {
            logger.error("Config file is not readable: {}", filePath);
            throw new ConfigLoadException("Config file is not readable: " + filePath, null);
        }
        if (!Files.isReadable(path)) {
            logger.error("Config file is not readable: {}", filePath);
            throw new ConfigLoadException("Config file is not readable: " + filePath, null);
        }

        SystemConfig cachedConfig = configCache.get(filePath);
        if (cachedConfig != null) {
            logger.debug("Returning cached configuration for: {}", filePath);
            return cachedConfig;
        }

        ObjectMapper mapper;
        if (filePath.toLowerCase().endsWith(".yaml") || filePath.toLowerCase().endsWith(".yml")) {
            mapper = new ObjectMapper(new YAMLFactory());
        } else {
            mapper = new ObjectMapper();
        }
        try {
            logger.info("Loading configuration from: {}", filePath);
            SystemConfig config = mapper.readValue(new File(filePath), SystemConfig.class);
            config.validate();
            configCache.put(filePath, config);
            logger.info("Successfully loaded and validated config from {}", filePath);
            return config;
        } catch (IOException e) {
            logger.error("Failed to load config from {}", filePath, e);
            throw new ConfigLoadException("Unable to load configuration: " + e.getMessage(), e);
        }
    }

    public static class ConfigLoadException extends Exception {
        public ConfigLoadException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}