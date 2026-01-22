package com.fspann.api;

import com.fspann.crypto.AesGcmCryptoService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class ReencryptionTest {

    private ForwardSecureANNSystem system;

    @BeforeEach
    void setUp() throws Exception {
        // Initialize system for testing re-encryption
        Path configPath = Paths.get("path/to/config.json");
        Path metadataPath = Paths.get("path/to/metadata");
        Path keysFilePath = Paths.get("path/to/keys");
        List<Integer> dimensions = List.of(128);

        system = new ForwardSecureANNSystem(
                configPath.toString(),
                "path/to/data",
                keysFilePath.toString(),
                dimensions,
                metadataPath,
                true,
                null,
                new AesGcmCryptoService(new SimpleMeterRegistry(), null, null),
                100000
        );
    }

    @Test
    void testReencryption() throws Exception {
        // Use reflection to call the private method
        Method method = ForwardSecureANNSystem.class.getDeclaredMethod("runSelectiveReencryptionIfNeeded");
        method.setAccessible(true);

        // Perform re-encryption
        method.invoke(system);

        // Check if re-encryption has been applied (based on your system's logic)
        assertTrue(system.getQueryCache().size() > 0); // Cache should have entries after re-encryption
    }

}

