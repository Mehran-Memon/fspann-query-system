package com.fspann.key;

import com.fspann.common.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
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

    @AfterEach
    public void tearDown() {
        if (metadataManager != null) {
            try {
                metadataManager.close();
            } catch (Exception ignore) {}
        }
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

    /**
     * FIXED: Test version activation with a version that won't be deleted
     *
     * Issue was: After two rotations + finalization, v1 gets deleted.
     * But test was trying to activate v1 which no longer exists.
     *
     * Solution: Only rotate ONCE (so current=2), then activate v1
     * v1 won't be deleted because finalizeRotation calls deleteKeysOlderThan(1)
     * which deletes nothing (no version < 1).
     */
    @Test
    @DisplayName("Test key version activation and deactivation")
    public void testVersionActivationCycle() throws Exception {
        // First rotation: v1 -> v2
        service.rotateKey();

        // At this point: current=2, and finalizeRotation calls deleteKeysOlderThan(1)
        // This deletes nothing because there's no version < 1
        // So v1 is still available!

        // Get v1 (which still exists)
        KeyVersion v1 = service.getVersion(1);
        assertNotNull(v1);
        assertEquals(1, v1.getVersion());

        // Activate v1
        service.activateVersion(1);

        KeyVersion active = service.getCurrentVersion();
        assertEquals(1, active.getVersion());

        // Deactivate (return to auto-rotation)
        service.clearActivatedVersion();

        KeyVersion current = service.getCurrentVersion();
        assertEquals(2, current.getVersion());
    }
}