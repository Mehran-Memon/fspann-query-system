package com.fspann.api;

import com.fspann.config.SystemConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
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
    private final String resolvedPath;
    private final String sha256;

    public ApiSystemConfig(String configFilePath) throws IOException {
        Objects.requireNonNull(configFilePath, "Config file path cannot be null");
        Path path = Paths.get(configFilePath).toAbsolutePath().normalize();
        if (!Files.isReadable(path)) {
            logger.error("Config file is not readable: {}", configFilePath);
            throw new IOException("Config file is not readable: " + configFilePath);
        }

        // Resolve (or fall back), assign field ONCE, and use it as the cache key
        String resolved;
        try {
            resolved = path.toRealPath(LinkOption.NOFOLLOW_LINKS).toString();
        } catch (IOException e) {
            resolved = path.toString();
            logger.debug("toRealPath failed for {} (using normalized path): {}", path, e.toString());
        }
        this.resolvedPath = resolved;
        String cacheKey = this.resolvedPath;

        // Compute provenance hash before any early return
        this.sha256 = computeSha256(this.resolvedPath);

        boolean refresh = Boolean.getBoolean("config.refresh");
        if (!refresh) {
            SystemConfig cachedConfig = configCache.get(cacheKey);
            if (cachedConfig != null) {
                logger.debug("Returning cached configuration for: {}", cacheKey);
                this.config = cachedConfig;
                return;
            }
        }

        try {
            logger.info("{} configuration from: {}", refresh ? "Reloading" : "Loading", cacheKey);
            this.config = SystemConfig.load(cacheKey, refresh);
            configCache.put(cacheKey, this.config);
        } catch (SystemConfig.ConfigLoadException e) {
            logger.error("Failed to load configuration: {}", configFilePath, e);
            throw new IOException("Failed to load configuration: " + configFilePath, e);
        }
    }

    public SystemConfig getConfig() {
        return config;
    }

    /** Clear both ApiSystemConfig and SystemConfig caches. Useful for tests/hot-reload. */
    public static void clearCache() {
        configCache.clear();
        SystemConfig.clearCache();
    }

    private static String computeSha256(String p) {
        try (java.io.InputStream in = java.nio.file.Files.newInputStream(java.nio.file.Paths.get(p))) {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-256");
            byte[] buf = new byte[8192];
            int r;
            while ((r = in.read(buf)) > 0) md.update(buf, 0, r);
            byte[] d = md.digest();
            StringBuilder sb = new StringBuilder(64);
            for (byte b : d) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            return "unknown";
        }
    }
}