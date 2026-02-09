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

// FIX: Removed @TestInstance(Lifecycle.PER_CLASS) to fix @TempDir injection
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
        if (root == null) throw new IllegalStateException("JUnit failed to inject TempDir");

        metaDir  = root.resolve("meta").toAbsolutePath();
        ptsDir   = root.resolve("pts").toAbsolutePath();
        ksFile   = root.resolve("keys.blob").toAbsolutePath();
        seedFile = root.resolve("seed.csv").toAbsolutePath();
        cfgFile  = root.resolve("cfg.json").toAbsolutePath();

        Files.createDirectories(metaDir);
        Files.createDirectories(ptsDir);
        Files.writeString(seedFile, "");

        // ... config writing logic ...

        cfg = SystemConfig.load(cfgFile.toString(), true);

        // Registry Setup (Keep minimal for ITs)
        GFunctionRegistry.reset();
        GFunctionRegistry.initialize(
                List.of(new double[DIM], new double[DIM]),
                DIM, cfg.getPaper().getM(), cfg.getPaper().getLambda(),
                cfg.getPaper().getSeed(), cfg.getPaper().getTables(), cfg.getPaper().getDivisions()
        );

        metadata = RocksDBMetadataManager.create(metaDir.toString(), ptsDir.toString());

        // ... KeyService & Crypto Setup ...

        system = new ForwardSecureANNSystem(
                cfgFile.toString(), seedFile.toString(), ksFile.toString(),
                List.of(DIM), root, false, metadata, crypto, 64
        );
    }

    @AfterEach
    protected void baseTearDown() {
        try {
            if (system != null) {
                system.setExitOnShutdown(false);
                system.shutdown();
            }
        } catch (Exception ignore) {}

        try {
            if (metadata != null) {
                metadata.close(); // Released meta\LOCK
            }
        } catch (Exception ignore) {}

        // Small delay to allow Windows OS to reclaim file handles
        try { Thread.sleep(50); } catch (InterruptedException ignore) {}
    }

    protected void indexClusteredData(int n) throws IOException {
        Random r = new Random(42);
        List<double[]> vectors = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            double[] v = new double[DIM];
            for (int d = 0; d < DIM; d++) v[d] = 5.0 + r.nextGaussian() * 0.1;
            vectors.add(v);
        }
        system.batchInsert(vectors, DIM);
        system.finalizeForSearch();
        metadata.flush();
    }
}