package com.fspann.it;

import com.fspann.api.ForwardSecureANNSystem;
import com.fspann.common.RocksDBMetadataManager;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.key.KeyManager;
import com.fspann.key.KeyRotationPolicy;
import com.fspann.key.KeyRotationServiceImpl;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.*;
import java.util.Comparator;
import java.util.Random;

public abstract class BaseUnifiedIT {

    @TempDir
    protected Path root;

    protected Path metaDir, ptsDir, ksFile, cfgFile, seedFile;
    protected RocksDBMetadataManager metadata;
    protected KeyRotationServiceImpl keyService;
    protected AesGcmCryptoService crypto;
    protected ForwardSecureANNSystem system;
    protected SystemConfig cfg;

    protected final int DIM = 8;

    @BeforeEach
    protected void baseSetup() throws Exception {
        metaDir = root.resolve("meta");
        ptsDir  = root.resolve("pts");
        ksFile  = root.resolve("keys.blob");
        seedFile = root.resolve("seed.csv");
        cfgFile = root.resolve("cfg.json");

        Files.createDirectories(metaDir);
        Files.createDirectories(ptsDir);

        Files.writeString(seedFile, "");
        Files.writeString(cfgFile, """
        {
          "paper": { "enabled": true, "m": 4, "divisions": 4, "lambda": 3, "seed": 42 },
          "output": { "exportArtifacts": false },
          "ratio": { "source": "none" },
          "reencryptionEnabled": true
        }
        """);

        cfg = SystemConfig.load(cfgFile.toString(), true);

        metadata = RocksDBMetadataManager.create(
                metaDir.toString(), ptsDir.toString()
        );

        KeyManager km = new KeyManager(ksFile.toString());
        keyService = new KeyRotationServiceImpl(
                km,
                new KeyRotationPolicy(Integer.MAX_VALUE, Long.MAX_VALUE),
                metaDir.toString(),
                metadata,
                null
        );

        crypto = new AesGcmCryptoService(
                new SimpleMeterRegistry(),
                keyService,
                metadata
        );
        keyService.setCryptoService(crypto);

        system = new ForwardSecureANNSystem(
                cfgFile.toString(),
                seedFile.toString(),
                ksFile.toString(),
                java.util.List.of(DIM),
                root,
                false,
                metadata,
                crypto,
                64
        );
    }

    protected void indexClusteredData(int n) throws IOException {
        Random r = new Random(42);
        for (int i = 0; i < n; i++) {
            double[] v = new double[DIM];
            for (int d = 0; d < DIM; d++)
                v[d] = 5.0 + r.nextGaussian() * 0.1;
            system.insert("v" + i, v, DIM);
        }

        system.finalizeForSearch();
        system.flushAll();

        // CRITICAL: Force RocksDB persistence
        try {
            metadata.flush();
            Thread.sleep(100);  // Give RocksDB time to write
        } catch (Exception e) {
            throw new RuntimeException("Failed to flush metadata", e);
        }
    }

    @AfterEach
    void cleanup() throws Exception {
        try {
            system.setExitOnShutdown(false);
            system.shutdown();
        } catch (Exception ignore) {}

        try {
            metadata.close();
        } catch (Exception ignore) {}

        Files.walk(root)
                .sorted(Comparator.reverseOrder())
                .forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (Exception ignore) {}
                });
    }
}