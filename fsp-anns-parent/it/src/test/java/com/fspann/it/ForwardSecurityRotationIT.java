package com.fspann.it;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.index.paper.PartitionedIndexService;
import com.fspann.key.*;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;

import javax.crypto.SecretKey;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class ForwardSecurityRotationIT {

    @Test
    void testOldKeyCannotDecryptAfterRotation() throws Exception {
        Path root = Files.createTempDirectory("fspann-rot");
        Path meta = root.resolve("meta");
        Path pts  = root.resolve("pts");

        Files.createDirectories(meta);
        Files.createDirectories(pts);

        RocksDBMetadataManager metadata =
                RocksDBMetadataManager.create(meta.toString(), pts.toString());

        Path cfgFile = root.resolve("cfg.json");
        Files.writeString(cfgFile, """
        {
          "paper": { "enabled": true, "divisions": 3, "m": 8, "seed": 7 },
          "reencryptionEnabled": true
        }
        """);

        SystemConfig cfg = SystemConfig.load(cfgFile.toString(), true);

        KeyManager km = new KeyManager(root.resolve("keys.blob").toString());
        KeyRotationServiceImpl keyService =
                new KeyRotationServiceImpl(
                        km,
                        new KeyRotationPolicy(1, Long.MAX_VALUE),
                        root.resolve("rot").toString(),
                        metadata,
                        null
                );

        AesGcmCryptoService crypto =
                new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadata);
        keyService.setCryptoService(crypto);

        PartitionedIndexService index =
                new PartitionedIndexService(
                        metadata,
                        cfg,
                        keyService,
                        crypto
                );

        index.insert("p1", new double[]{1, 2, 3});
        index.finalizeForSearch();

        EncryptedPoint before = metadata.loadEncryptedPoint("p1");
        int oldVersion = keyService.getCurrentVersion().getVersion();
        SecretKey oldKey = keyService.getCurrentVersion().getKey();

        keyService.rotateKeyOnly();
        keyService.reEncryptAll();

        EncryptedPoint after = metadata.loadEncryptedPoint("p1");

        assertThrows(
                Exception.class,
                () -> crypto.decryptFromPoint(after, oldKey)
        );

        double[] vec = crypto.decryptFromPoint(after, null);
        assertEquals(3, vec.length);
    }
}
