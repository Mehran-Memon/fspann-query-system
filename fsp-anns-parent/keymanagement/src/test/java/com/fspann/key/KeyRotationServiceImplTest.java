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

@DisplayName("KeyRotationServiceImpl Unit Tests")
public class KeyRotationServiceImplTest {

    private KeyRotationServiceImpl service;
    private KeyManager keyManager;
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

        KeyRotationPolicy policy = new KeyRotationPolicy(1000, 60_000);

        service = new KeyRotationServiceImpl(
                keyManager,
                policy,
                metaDbPath.toString(),
                metadataManager,
                null
        );
    }

    @Test
    @DisplayName("Test KeyRotationServiceImpl returns current version")
    public void testGetCurrentVersion() {
        KeyVersion current = service.getCurrentVersion();
        assertNotNull(current);
        assertEquals(1, current.getVersion());
    }

    @Test
    @DisplayName("Test KeyRotationServiceImpl increments operations")
    public void testIncrementOperation() {
        service.incrementOperation();
        service.incrementOperation();
    }

    @Test
    @DisplayName("Test KeyRotationServiceImpl rotates key manually")
    public void testRotateKey() {
        KeyVersion v1 = service.getCurrentVersion();
        assertEquals(1, v1.getVersion());

        KeyVersion v2 = service.rotateKey();
        assertEquals(2, v2.getVersion());

        KeyVersion current = service.getCurrentVersion();
        assertEquals(2, current.getVersion());
    }

    @Test
    @DisplayName("Test KeyRotationServiceImpl force rotate")
    public void testForceRotateNow() {
        KeyVersion v1 = service.getCurrentVersion();
        service.forceRotateNow();
        KeyVersion v2 = service.getCurrentVersion();

        assertTrue(v2.getVersion() > v1.getVersion());
    }

    @Test
    @DisplayName("Test KeyRotationServiceImpl activates version")
    public void testActivateVersion() throws Exception {
        service.rotateKey();

        KeyVersion v1 = service.getPreviousVersion();

        boolean success = service.activateVersion(v1.getVersion());
        assertTrue(success);

        KeyVersion current = service.getCurrentVersion();
        assertEquals(1, current.getVersion());
    }

    @Test
    @DisplayName("Test KeyRotationServiceImpl clears activated version")
    public void testClearActivatedVersion() throws Exception {
        service.rotateKey();
        KeyVersion v1 = service.getPreviousVersion();

        service.activateVersion(v1.getVersion());
        service.clearActivatedVersion();

        KeyVersion current = service.getCurrentVersion();
        assertEquals(2, current.getVersion());
    }

    @Test
    @DisplayName("Test KeyRotationServiceImpl get version by number")
    public void testGetVersion() {
        service.rotateKey();
        service.rotateKey();

        KeyVersion v2 = service.getVersion(2);
        assertNotNull(v2);
        assertEquals(2, v2.getVersion());
    }

    @Test
    @DisplayName("Test KeyRotationServiceImpl get version throws for unknown")
    public void testGetUnknownVersion() {
        assertThrows(IllegalArgumentException.class, () -> service.getVersion(999));
    }

    @Test
    @DisplayName("Test KeyRotationServiceImpl rotation policy enforcement")
    public void testRotationPolicyNotTriggeredImmediately() {
        service.incrementOperation();
    }
}