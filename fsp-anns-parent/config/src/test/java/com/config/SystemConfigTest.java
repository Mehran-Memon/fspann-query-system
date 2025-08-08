package com.config;

import com.fspann.config.SystemConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class SystemConfigTest {

    @Test
    void loadYamlAndCapping(@TempDir Path dir) throws Exception {
        Path cfg = dir.resolve("conf.yaml");
        String yaml = """
            numShards: 999999
            opsThreshold: 2000000000
            ageThresholdMs: 999999999999
            reEncBatchSize: 999999
            profilerEnabled: false
            """;
        Files.writeString(cfg, yaml);

        SystemConfig.clearCache();
        SystemConfig c = SystemConfig.load(cfg.toString(), true);

        assertEquals(1024, c.getNumShards());                // capped
        assertEquals(1_000_000_000L, c.getOpsThreshold());   // capped
        assertEquals(30L*24*60*60*1000, c.getAgeThresholdMs());// capped
        assertEquals(10_000, c.getReEncBatchSize());         // capped
        assertFalse(c.isProfilerEnabled());                  // value respected
    }

    @Test
    void cachingAndRefresh(@TempDir Path dir) throws Exception {
        Path cfg = dir.resolve("conf.json");
        Files.writeString(cfg, """
            {"numShards":16,"opsThreshold":1000,"ageThresholdMs":1000,"reEncBatchSize":100,"profilerEnabled":true}
            """);

        SystemConfig.clearCache();
        SystemConfig first = SystemConfig.load(cfg.toString());
        assertEquals(16, first.getNumShards());

        // Modify file
        Files.writeString(cfg, """
            {"numShards":64,"opsThreshold":2000,"ageThresholdMs":2000,"reEncBatchSize":200,"profilerEnabled":false}
            """);

        // Without refresh -> still cached
        SystemConfig cached = SystemConfig.load(cfg.toString());
        assertEquals(16, cached.getNumShards());

        // With refresh -> new values
        SystemConfig refreshed = SystemConfig.load(cfg.toString(), true);
        assertEquals(64, refreshed.getNumShards());
        assertFalse(refreshed.isProfilerEnabled());
    }

    @Test
    void unreadablePathThrows() {
        assertThrows(SystemConfig.ConfigLoadException.class,
                () -> SystemConfig.load("Z:/definitely/not/here/config.json"));
    }
}
