package com.config;

import com.fspann.config.SystemConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class SystemConfigMalformedInputTest {

    @Test
    void load_nonExistingFile_throwsConfigLoadException() {
        String bogusPath = "this/does/not/exist/config.json";

        assertThrows(SystemConfig.ConfigLoadException.class, () ->
                        SystemConfig.load(bogusPath, true),
                "Expected ConfigLoadException for non-existing file");
    }

    @Test
    void load_directoryInsteadOfFile_throwsConfigLoadException(@TempDir Path tempDir) {
        // tempDir itself is a directory; pass it directly
        assertThrows(SystemConfig.ConfigLoadException.class, () ->
                        SystemConfig.load(tempDir.toString(), true),
                "Expected ConfigLoadException when path is a directory, not a regular file");
    }

    @Test
    void load_malformedJson_throwsConfigLoadException(@TempDir Path tempDir) throws IOException {
        Path cfgPath = tempDir.resolve("bad.json");
        Files.writeString(cfgPath, "this is not valid JSON {", StandardCharsets.UTF_8);

        assertThrows(SystemConfig.ConfigLoadException.class, () ->
                        SystemConfig.load(cfgPath.toString(), true),
                "Expected ConfigLoadException for malformed JSON");
    }
}
