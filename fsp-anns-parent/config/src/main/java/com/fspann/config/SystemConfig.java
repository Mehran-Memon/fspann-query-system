package com.fspann.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Application configuration loaded from a JSON or YAML file.
 * Provides shard and rotation settings, re-encryption batch size, and profiler toggle.
 */
public class SystemConfig {
    private static final Logger logger = LoggerFactory.getLogger(SystemConfig.class);

    // caps
    private static final int  MAX_SHARDS          = 8192;
    private static final long MAX_OPS_THRESHOLD   = 1_000_000_000L;
    private static final long MAX_AGE_THRESHOLD_MS= 30L * 24 * 60 * 60 * 1000; // 30 days
    private static final int  MAX_REENC_BATCH_SIZE= 10_000;

    // cache per absolute normalized path
    private static final ConcurrentMap<String, SystemConfig> configCache = new ConcurrentHashMap<>();

    @JsonProperty("numShards")      private int  numShards      = 32;
    @JsonProperty("numShards")      private int  numTables      = 8;
    @JsonProperty("opsThreshold")   private long opsThreshold   = 500_000_000L;
    @JsonProperty("ageThresholdMs") private long ageThresholdMs = 7L * 24 * 60 * 60 * 1000; // 7 days
    @JsonProperty("reEncBatchSize") private int  reEncBatchSize = 2_000;
    @JsonProperty("profilerEnabled")private boolean profilerEnabled = true;

    public SystemConfig() {} // for Jackson

    // Getters
    public int getNumShards() { return numShards; }
    public int getNumTables() { return numTables; }
    public long getOpsThreshold() { return opsThreshold; }
    public long getAgeThresholdMs() { return ageThresholdMs; }
    public int getReEncBatchSize() { return reEncBatchSize; }
    public boolean isProfilerEnabled() { return profilerEnabled; }

    public void validate() throws ConfigLoadException {
        if (numShards <= 0) throw new ConfigLoadException("numShards must be positive", null);
        if (numShards > MAX_SHARDS) {
            logger.warn("numShards {} exceeds maximum {}, capping", numShards, MAX_SHARDS);
            numShards = MAX_SHARDS;
        }
        if (opsThreshold <= 0) throw new ConfigLoadException("opsThreshold must be positive", null);
        if (opsThreshold > MAX_OPS_THRESHOLD) {
            logger.warn("opsThreshold {} exceeds maximum {}, capping", opsThreshold, MAX_OPS_THRESHOLD);
            opsThreshold = MAX_OPS_THRESHOLD;
        }
        if (ageThresholdMs <= 0) throw new ConfigLoadException("ageThresholdMs must be positive", null);
        if (ageThresholdMs > MAX_AGE_THRESHOLD_MS) {
            logger.warn("ageThresholdMs {} exceeds maximum {}, capping", ageThresholdMs, MAX_AGE_THRESHOLD_MS);
            ageThresholdMs = MAX_AGE_THRESHOLD_MS;
        }
        if (reEncBatchSize <= 0) throw new ConfigLoadException("reEncBatchSize must be positive", null);
        if (reEncBatchSize > MAX_REENC_BATCH_SIZE) {
            logger.warn("reEncBatchSize {} exceeds maximum {}, capping", reEncBatchSize, MAX_REENC_BATCH_SIZE);
            reEncBatchSize = MAX_REENC_BATCH_SIZE;
        }
    }

    /** Load configuration (JSON or YAML). Uses an internal cache. */
    public static SystemConfig load(String filePath) throws ConfigLoadException {
        return load(filePath, false);
    }

    /**
     * Load configuration with optional cache refresh.
     * @param filePath path to config file (json/yaml)
     * @param refresh  if true, bypass the cache and reload from disk
     */
    public static SystemConfig load(String filePath, boolean refresh) throws ConfigLoadException {
        Objects.requireNonNull(filePath, "Config file path cannot be null");
        Path path = Paths.get(filePath).toAbsolutePath().normalize();
        String cacheKey = path.toString();

        if (!Files.isReadable(path)) {
            logger.error("Config file is not readable: {}", path);
            throw new ConfigLoadException("Config file is not readable: " + path, null);
        }

        if (!refresh) {
            SystemConfig cached = configCache.get(cacheKey);
            if (cached != null) {
                logger.debug("Returning cached configuration for: {}", cacheKey);
                return cached;
            }
        }

        ObjectMapper mapper = (cacheKey.toLowerCase().endsWith(".yaml") || cacheKey.toLowerCase().endsWith(".yml"))
                ? new ObjectMapper(new YAMLFactory())
                : new ObjectMapper();

        try {
            logger.info("Loading configuration from: {}", cacheKey);
            SystemConfig config = mapper.readValue(new File(cacheKey), SystemConfig.class);
            config.validate();
            configCache.put(cacheKey, config);
            logger.info("Successfully loaded and validated config from {}", cacheKey);
            return config;
        } catch (IOException e) {
            logger.error("Failed to load config from {}", cacheKey, e);
            throw new ConfigLoadException("Unable to load configuration: " + e.getMessage(), e);
        }
    }

    /** Clear the in-memory cache (useful for tests). */
    public static void clearCache() {
        configCache.clear();
    }

    public static class ConfigLoadException extends Exception {
        public ConfigLoadException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
