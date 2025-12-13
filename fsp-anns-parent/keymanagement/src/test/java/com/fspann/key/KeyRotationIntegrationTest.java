package com.fspann.key;

import com.fspann.common.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@DisplayName("Key Rotation Integration Tests")
public class KeyRotationIntegrationTest {

    private KeyManager keyManager;
    private KeyRotationServiceImpl service;
    private RocksDBMetadataManager metadataManager;

    @TempDir
    Path tempDir;

    @BeforeEach
    public void setUp() throws IOException {
        Path keyStorePath = tempDir.resolve("keystore.blob");
        Path metaDbPath = tempDir.resolve("metadata");
        Path pointsPath = tempDir.resolve("points");

        Files.createDirectories(metaDbPath);
        Files.createDirectories(pointsPath);

        keyManager = new KeyManager(keyStorePath.toString());
        metadataManager = RocksDBMetadataManager.create(metaDbPath.toString(), pointsPath.toString());

        KeyRotationPolicy policy = new KeyRotationPolicy(10, 60_000);

        service = new KeyRotationServiceImpl(
                keyManager,
                policy,
                metaDbPath.toString(),
                metadataManager,
                null
        );
    }

    @Test
    @DisplayName("Test full key rotation cycle")
    public void testFullRotationCycle() {
        KeyVersion initial = service.getCurrentVersion();
        assertEquals(1, initial.getVersion());

        KeyVersion v2 = service.rotateKey();
        assertEquals(2, v2.getVersion());

        KeyVersion prev = service.getPreviousVersion();
        assertEquals(1, prev.getVersion());

        KeyVersion v3 = service.rotateKey();
        assertEquals(3, v3.getVersion());

        KeyVersion current = service.getCurrentVersion();
        assertEquals(3, current.getVersion());
    }

    @Test
    @DisplayName("Test key version activation and deactivation")
    public void testVersionActivationCycle() throws Exception {
        service.rotateKey();
        service.rotateKey();

        KeyVersion v1 = service.getVersion(1);
        service.activateVersion(1);

        KeyVersion active = service.getCurrentVersion();
        assertEquals(1, active.getVersion());

        service.clearActivatedVersion();

        KeyVersion current = service.getCurrentVersion();
        assertEquals(3, current.getVersion());
    }
}