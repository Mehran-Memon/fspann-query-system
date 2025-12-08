package com.fspann.it;

import com.fspann.api.ForwardSecureANNSystem;
import com.fspann.common.RocksDBMetadataManager;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.key.KeyManager;
import com.fspann.key.KeyRotationPolicy;
import com.fspann.key.KeyRotationServiceImpl;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public abstract class BaseSystemIT {

    protected ForwardSecureANNSystem sys;
    protected RocksDBMetadataManager meta;
    protected KeyRotationServiceImpl keySvc;

    protected ForwardSecureANNSystem build(Path temp, String json) throws Exception {

        Path cfg = temp.resolve("conf.json");
        Files.writeString(cfg, json);

        Path keysDir = temp.resolve("keys");
        Path metaDir = temp.resolve("meta");
        Path ptDir   = temp.resolve("pts");
        Files.createDirectories(keysDir);
        Files.createDirectories(metaDir);
        Files.createDirectories(ptDir);

        meta = RocksDBMetadataManager.create(metaDir.toString(), ptDir.toString());

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
                temp.resolve("dummy.csv").toString(),
                keysDir.toString(),
                List.of(2),
                temp,
                true,
                meta,
                crypto,
                128
        );
        sys.setExitOnShutdown(false);
        return sys;
    }
}
