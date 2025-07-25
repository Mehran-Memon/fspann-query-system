package com.fspann.api;

import com.fspann.config.SystemConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApiSystemConfig {
    private static final Logger logger = LoggerFactory.getLogger(ApiSystemConfig.class);
    private static final ConcurrentMap<String, SystemConfig> configCache = new ConcurrentHashMap<>();
    private final SystemConfig config;

    public ApiSystemConfig(String configFilePath) throws IOException {
        Objects.requireNonNull(configFilePath, "Config file path cannot be null");
        Path path = Paths.get(configFilePath).normalize();
        if (!Files.isReadable(path)) {
            logger.error("Config file is not readable: {}", configFilePath);
            throw new IOException("Config file is not readable: " + configFilePath);
        }

        SystemConfig cachedConfig = configCache.get(configFilePath);
        if (cachedConfig != null) {
            logger.debug("Returning cached configuration for: {}", configFilePath);
            this.config = cachedConfig;
            return;
        }

        try {
            logger.info("Loading configuration from: {}", configFilePath);
            this.config = SystemConfig.load(configFilePath);
            configCache.put(configFilePath, this.config);
        } catch (SystemConfig.ConfigLoadException e) {
            logger.error("Failed to load configuration: {}", configFilePath, e);
            throw new IOException("Failed to load configuration: " + configFilePath, e);
        }
    }

    public SystemConfig getConfig() {
        return config;
    }
}