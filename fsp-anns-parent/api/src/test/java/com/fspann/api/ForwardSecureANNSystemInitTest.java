package com.fspann.api;

import com.fspann.common.*;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.crypto.CryptoService;
import com.fspann.key.KeyManager;
import com.fspann.key.KeyRotationPolicy;
import com.fspann.key.KeyRotationServiceImpl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

@DisplayName("ForwardSecureANNSystem Initialization Tests")
public class ForwardSecureANNSystemInitTest {

    private Path configPath;
    private Path metadataPath;
    private Path keyStorePath;
    private RocksDBMetadataManager metadataManager;
    private CryptoService cryptoService;

    @TempDir
    Path tempDir;

    @BeforeEach
    public void setUp() throws IOException {
        configPath = tempDir.resolve("config.yaml");
        Files.writeString(configPath, """
                eval:
                  computePrecision: true
                  kVariants: [10, 100]
                output:
                  resultsDir: results
                paper:
                  m: 4
                  lambda: 2
                  divisions: 4
                  seed: 12345
                """);

        metadataPath = tempDir.resolve("metadata");
        keyStorePath = tempDir.resolve("keystore.blob");

        Files.createDirectories(metadataPath);
        Files.createDirectories(metadataPath.resolve("metadata"));
        Files.createDirectories(metadataPath.resolve("points"));

        metadataManager = RocksDBMetadataManager.create(
                metadataPath.resolve("metadata").toString(),
                metadataPath.resolve("points").toString()
        );

        KeyManager keyManager = new KeyManager(keyStorePath.toString());
        KeyRotationServiceImpl keyService = new KeyRotationServiceImpl(
                keyManager,
                new KeyRotationPolicy(1000, 60_000),
                metadataPath.resolve("metadata").toString(),
                metadataManager,
                null
        );

        cryptoService = new AesGcmCryptoService(
                new SimpleMeterRegistry(),
                keyService,
                metadataManager
        );

        keyService.setCryptoService(cryptoService);
    }

    @Test
    @DisplayName("Test ForwardSecureANNSystem initializes successfully")
    public void testSystemInitialization() throws Exception {
        ForwardSecureANNSystem system = new ForwardSecureANNSystem(
                configPath.toString(),
                "POINTS_ONLY",
                keyStorePath.toString(),
                Arrays.asList(128),
                metadataPath,
                false,
                metadataManager,
                cryptoService,
                100
        );

        assertNotNull(system);
        system.setExitOnShutdown(false);
        system.shutdown();
    }

    @Test
    @DisplayName("Test ForwardSecureANNSystem requires non-null config")
    public void testNullConfigThrows() throws Exception {
        assertThrows(Exception.class, () -> new ForwardSecureANNSystem(
                null,
                "data.fvecs",
                keyStorePath.toString(),
                Arrays.asList(128),
                metadataPath,
                false,
                metadataManager,
                cryptoService,
                100
        ));
    }

    @Test
    @DisplayName("Test ForwardSecureANNSystem requires non-null keystore")
    public void testNullKeyStoreThrows() throws Exception {
        assertThrows(Exception.class, () -> new ForwardSecureANNSystem(
                configPath.toString(),
                "data.fvecs",
                null,
                Arrays.asList(128),
                metadataPath,
                false,
                metadataManager,
                cryptoService,
                100
        ));
    }

    @Test
    @DisplayName("Test ForwardSecureANNSystem requires non-null dimensions")
    public void testNullDimensionsThrows() throws Exception {
        assertThrows(Exception.class, () -> new ForwardSecureANNSystem(
                configPath.toString(),
                "data.fvecs",
                keyStorePath.toString(),
                null,
                metadataPath,
                false,
                metadataManager,
                cryptoService,
                100
        ));
    }

    @Test
    @DisplayName("Test ForwardSecureANNSystem requires non-empty dimensions")
    public void testEmptyDimensionsThrows() throws Exception {
        assertThrows(Exception.class, () -> new ForwardSecureANNSystem(
                configPath.toString(),
                "data.fvecs",
                keyStorePath.toString(),
                Collections.emptyList(),
                metadataPath,
                false,
                metadataManager,
                cryptoService,
                100
        ));
    }

    @Test
    @DisplayName("Test ForwardSecureANNSystem requires positive batch size")
    public void testNegativeBatchSizeThrows() throws Exception {
        assertThrows(Exception.class, () -> new ForwardSecureANNSystem(
                configPath.toString(),
                "data.fvecs",
                keyStorePath.toString(),
                Arrays.asList(128),
                metadataPath,
                false,
                metadataManager,
                cryptoService,
                -1
        ));
    }
}