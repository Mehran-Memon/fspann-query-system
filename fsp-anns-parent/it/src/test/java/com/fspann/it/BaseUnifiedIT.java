package com.fspann.it;

import com.fspann.api.ForwardSecureANNSystem;
import com.fspann.common.RocksDBMetadataManager;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.index.paper.GFunctionRegistry;
import com.fspann.key.KeyManager;
import com.fspann.key.KeyRotationPolicy;
import com.fspann.key.KeyRotationServiceImpl;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class BaseUnifiedIT {

    @TempDir
    protected Path root;

    protected Path metaDir, ptsDir, ksFile, cfgFile, seedFile;
    protected RocksDBMetadataManager metadata;
    protected KeyRotationServiceImpl keyService;
    protected AesGcmCryptoService crypto;
    protected ForwardSecureANNSystem system;
    protected SystemConfig cfg;

    protected static final int DIM = 8;

    @BeforeEach
    protected void baseSetup() throws Exception {

        metaDir  = root.resolve("meta");
        ptsDir   = root.resolve("pts");
        ksFile   = root.resolve("keys.blob");
        seedFile = root.resolve("seed.csv");
        cfgFile  = root.resolve("cfg.json");

        Files.createDirectories(metaDir);
        Files.createDirectories(ptsDir);
        Files.writeString(seedFile, "");

        Files.writeString(cfgFile, """
        {
          "paper": {
            "enabled": true,
            "m": 4,
            "divisions": 4,
            "lambda": 3,
            "tables": 2,
            "seed": 42
          },
          "reencryption": { "enabled": true }
        }
        """);

        cfg = SystemConfig.load(cfgFile.toString(), true);

        // -----------------------------
        // MSANNP REGISTRY (MANDATORY)
        // -----------------------------
        GFunctionRegistry.reset();
        GFunctionRegistry.initialize(
                List.of(
                        new double[]{1,2,3,4,5,6,7,8},
                        new double[]{2,3,4,5,6,7,8,9}
                ),
                DIM,
                cfg.getPaper().getM(),
                cfg.getPaper().getLambda(),
                cfg.getPaper().getSeed(),
                cfg.getPaper().getTables(),
                cfg.getPaper().getDivisions()
        );

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
                List.of(DIM),
                root,
                false,
                metadata,
                crypto,
                64
        );
    }

    protected void indexClusteredData(int n) throws IOException {

        Random r = new Random(42);
        int count = Math.max(n, 1024); // stabilize registry behavior

        List<double[]> vectors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            double[] v = new double[DIM];
            for (int d = 0; d < DIM; d++) {
                v[d] = 5.0 + r.nextGaussian() * 0.1;
            }
            vectors.add(v);
        }

        system.batchInsert(vectors, DIM);
        system.finalizeForSearch();
        metadata.flush();
    }

    // --------------------------------------------------
    // SINGLE CLEANUP (NO PER-TEST SHUTDOWN)
    // --------------------------------------------------
    @AfterAll
    void cleanupOnce() {
        try {
            if (system != null) {
                system.setExitOnShutdown(false);
                system.shutdown();
            }
        } catch (Exception ignore) {}

        try {
            if (metadata != null) metadata.close();
        } catch (Exception ignore) {}
    }
}
