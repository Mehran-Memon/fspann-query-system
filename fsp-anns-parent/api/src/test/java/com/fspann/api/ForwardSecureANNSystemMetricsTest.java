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

@DisplayName("ForwardSecureANNSystem Metrics Tests")
public class ForwardSecureANNSystemMetricsTest {

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
    @DisplayName("Test get indexed vector count")
    public void testGetIndexedCount() throws Exception {
        for (int i = 0; i < 10; i++) {
            double[] v = new double[128];
            system.insert("v-" + i, v, 128);
        }

        int count = system.getIndexedVectorCount();
        assertEquals(10, count);

        system.shutdown();
    }

    @Test
    @DisplayName("Test get last insert milliseconds")
    public void testGetLastInsertMs() throws Exception {
        double[] v = new double[128];
        system.insert("v-1", v, 128);

        long ms = system.lastInsertMs();
        assertTrue(ms >= 0);

        system.shutdown();
    }

    @Test
    @DisplayName("Test flush threshold")
    public void testFlushThreshold() throws Exception {
        int threshold = system.flushThreshold();
        assertTrue(threshold > 0);

        system.shutdown();
    }
}