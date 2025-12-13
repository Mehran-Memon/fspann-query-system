package com.fspann.it;

import com.fspann.api.ForwardSecureANNSystem;
import com.fspann.common.RocksDBMetadataManager;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.key.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public abstract class BaseSystemIT {

    protected ForwardSecureANNSystem sys;
    protected RocksDBMetadataManager meta;
    protected KeyRotationServiceImpl keySvc;

    protected static String minimalCfgJson() {
        return """
        {
          "paper": { "enabled": true, "m": 3, "divisions": 3, "lambda": 3, "seed": 13 },
          "lsh": { "numTables": 0, "rowsPerBand": 0, "probeShards": 0 },
          "ratio": { "source": "base" },
          "output": { "exportArtifacts": false },
          "reencryption": { "enabled": false }
        }
        """;
    }

    protected ForwardSecureANNSystem build(Path temp) throws Exception {

        Path cfg = temp.resolve("cfg.json");
        Files.writeString(cfg, minimalCfgJson());

        Path keysDir = temp.resolve("keys");
        Path metaDir = temp.resolve("meta");
        Path ptsDir  = temp.resolve("pts");

        Files.createDirectories(keysDir);
        Files.createDirectories(metaDir);
        Files.createDirectories(ptsDir);

        meta = RocksDBMetadataManager.create(metaDir.toString(), ptsDir.toString());

        KeyManager km = new KeyManager(keysDir.resolve("ks.blob").toString());
        keySvc = new KeyRotationServiceImpl(
                km,
                new KeyRotationPolicy(999999, 999999),
                metaDir.toString(),
                meta,
                null
        );

        AesGcmCryptoService crypto = new AesGcmCryptoService(new SimpleMeterRegistry(), keySvc, meta);
        keySvc.setCryptoService(crypto);

        sys = new ForwardSecureANNSystem(
                cfg.toString(),
                temp.resolve("ignored.csv").toString(),
                keysDir.toString(),
                List.of(4),
                temp,
                false,
                meta,
                crypto,
                128
        );

        sys.setExitOnShutdown(false);
        return sys;
    }
}