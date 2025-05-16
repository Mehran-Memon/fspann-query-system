package com.fspann.api;

import com.fspann.config.SystemConfig;

/**
 * Loads system configuration from a JSON/YAML file.
 */
public class ApiSystemConfig {
    private final SystemConfig config;

    public ApiSystemConfig(String configFilePath) throws Exception {
        this.config = SystemConfig.load(configFilePath);
    }

    public SystemConfig getConfig() {
        return config;
    }
}