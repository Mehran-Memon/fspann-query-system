package com.fspann.it;

import com.fspann.api.ForwardSecureANNSystem;
import com.fspann.common.RocksDBMetadataManager;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.key.KeyManager;
import com.fspann.key.KeyRotationPolicy;
import com.fspann.key.KeyRotationServiceImpl;
import com.fspann.loader.GroundtruthManager;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Objects;
import java.util.Random;

public abstract class BaseUnifiedIT {

    protected Path root, metaDir, ptsDir, ksFile, cfgFile, seedFile;

    protected RocksDBMetadataManager metadata;
    protected KeyRotationServiceImpl keyService;
    protected AesGcmCryptoService crypto;
    protected ForwardSecureANNSystem system;
    protected SystemConfig cfg;

    protected final int DIM = 8;
    private KeyManager keyManager;
    private GroundtruthManager groundtruth;

    @BeforeEach
    protected void baseSetup() throws Exception {
        Objects.requireNonNull(root);
        Objects.requireNonNull(cfgFile);

        this.root = root;
        this.cfg = SystemConfig.load(cfgFile.toString(), true);

        // --- Metadata ---
        Path meta = root.resolve("metadata");
        Path pts  = root.resolve("points");
        Files.createDirectories(meta);
        Files.createDirectories(pts);

        this.metadata = RocksDBMetadataManager.create(
                meta.toString(), pts.toString()
        );

        // --- Keys ---
        this.keyManager = new KeyManager(root.resolve("keys.blob").toString());
        this.keyService = new KeyRotationServiceImpl(
                keyManager,
                new KeyRotationPolicy(Integer.MAX_VALUE, Long.MAX_VALUE),
                meta.toString(),
                metadata,
                null
        );

        this.crypto = new AesGcmCryptoService(
                new SimpleMeterRegistry(),
                keyService,
                metadata
        );
        keyService.setCryptoService(crypto);

        // --- Ratio & GT are OPTIONAL ---
        if (cfg.getRatio() != null && cfg.getRatio().isEnabled()) {
            if (cfg.getRatio().requiresGroundtruth()) {
                this.groundtruth = new GroundtruthManager();
            }
        }

        // --- NEVER auto-init GFunctions here ---
        // GFunctionRegistry must be initialized ONLY after dimension is known
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
    }

    @AfterEach
    void cleanup() throws Exception {
        try { system.setExitOnShutdown(false); system.shutdown(); } catch (Exception ignore) {}
        try { metadata.close(); } catch (Exception ignore) {}
        Files.walk(root).sorted(Comparator.reverseOrder())
                .forEach(p -> { try { Files.deleteIfExists(p); } catch (Exception ignore) {} });
    }
}

