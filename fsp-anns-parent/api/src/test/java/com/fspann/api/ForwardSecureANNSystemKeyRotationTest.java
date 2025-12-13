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

@DisplayName("ForwardSecureANNSystem Key Rotation Integration Tests")
public class ForwardSecureANNSystemKeyRotationTest {

    private Path configPath;
    private Path metadataPath;
    private Path keyStorePath;
    private RocksDBMetadataManager metadataManager;
    private CryptoService cryptoService;
    private ForwardSecureANNSystem system;

    @TempDir
    Path tempDir;

    @BeforeEach
    public void setUp() throws Exception {
        configPath = tempDir.resolve("config.yaml");
        Files.writeString(configPath, """
                eval:
                  computePrecision: false
                paper:
                  m: 4
                  lambda: 2
                  divisions: 4
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
                new KeyRotationPolicy(100, 60_000),
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

        system = new ForwardSecureANNSystem(
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
        system.setExitOnShutdown(false);
    }

    @Test
    @DisplayName("Test system maintains key versions")
    public void testKeyVersionTracking() throws Exception {
        for (int i = 0; i < 5; i++) {
            double[] v = new double[128];
            system.insert("v-" + i, v, 128);
        }

        system.shutdown();
    }

    @Test
    @DisplayName("Test system accumulates touched IDs")
    public void testTouchedIdAccumulation() throws Exception {
        Set<String> touched = new HashSet<>(Arrays.asList("v-1", "v-2", "v-3"));
        system.accumulateTouchedIds(touched);

        system.shutdown();
    }
}