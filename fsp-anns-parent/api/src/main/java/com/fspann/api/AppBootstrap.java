package com.fspann.api;

import com.fspann.common.EncryptedPointBuffer;
import com.fspann.common.RocksDBMetadataManager;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.crypto.CryptoService;
import com.fspann.index.service.SecureLSHIndexService;
import com.fspann.key.KeyManager;
import com.fspann.key.KeyRotationPolicy;
import com.fspann.key.KeyRotationServiceImpl;
import com.fspann.query.service.QueryServiceImpl;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public final class AppBootstrap {

    public static final class Components {
        public final SystemConfig config;
        public final RocksDBMetadataManager metadata;
        public final KeyManager keyManager;
        public final KeyRotationServiceImpl keyService;
        public final CryptoService crypto;
        public final SecureLSHIndexService indexService;
        public final QueryServiceImpl queryService;

        Components(SystemConfig cfg,
                   RocksDBMetadataManager metadata,
                   KeyManager km,
                   KeyRotationServiceImpl ks,
                   CryptoService crypto,
                   SecureLSHIndexService idx,
                   QueryServiceImpl qs) {
            this.config = cfg;
            this.metadata = metadata;
            this.keyManager = km;
            this.keyService = ks;
            this.crypto = crypto;
            this.indexService = idx;
            this.queryService = qs;
        }
    }

    public static Components init(SystemConfig cfg, Path baseDir) throws IOException {
        Objects.requireNonNull(cfg, "cfg");
        Objects.requireNonNull(baseDir, "baseDir");
        Files.createDirectories(baseDir);

        // Resolve paths under baseDir
        Path dbPath     = baseDir.resolve("metadata/rocksdb");
        Path pointsPath = baseDir.resolve("metadata/points");
        Path keysPath   = baseDir.resolve("keys/keystore.blob");
        Path rotateMeta = baseDir.resolve("rotation");

        Files.createDirectories(dbPath);
        Files.createDirectories(pointsPath);
        Files.createDirectories(keysPath.getParent());
        Files.createDirectories(rotateMeta);

        // Metadata
        RocksDBMetadataManager metadata = RocksDBMetadataManager.create(
                dbPath.toString(), pointsPath.toString());

        // Keys & rotation
        KeyManager km = new KeyManager(keysPath.toString());
        int opsCap = (int) Math.min(Integer.MAX_VALUE, cfg.getOpsThreshold());
        KeyRotationPolicy policy = new KeyRotationPolicy(opsCap, cfg.getAgeThresholdMs());
        KeyRotationServiceImpl keySvc = new KeyRotationServiceImpl(
                km, policy, rotateMeta.toString(), metadata, /* crypto */ null);

        // Crypto
        CryptoService crypto = new AesGcmCryptoService(new SimpleMeterRegistry(), keySvc, metadata);

        // Index service wiring (map config → ctor inputs, no config type leakage into index module)
        int numBuckets  = Math.max(1, cfg.getNumShards());   // map shards → buckets
        int numTables   = 4;                                 // keep default (could expose later in config)
        int flushThresh = Math.max(1, cfg.getReEncBatchSize());

        EncryptedPointBuffer buffer = new EncryptedPointBuffer(pointsPath.toString(), metadata, flushThresh);

        SecureLSHIndexService indexService = new SecureLSHIndexService(
                crypto, keySvc, metadata,
                /* index */ null, /* lsh */ null, buffer,
                /* defaultNumBuckets */ numBuckets,
                /* defaultNumTables  */ numTables
        );

        // Wire back-pointers for rotation flow
        keySvc.setCryptoService(crypto);
        keySvc.setIndexService(indexService);

        // Query service
        QueryServiceImpl queryService = new QueryServiceImpl(indexService, crypto, keySvc);

        return new Components(cfg, metadata, km, keySvc, crypto, indexService, queryService);
    }

    private AppBootstrap() {}
}
