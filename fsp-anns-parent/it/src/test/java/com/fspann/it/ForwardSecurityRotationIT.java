package com.fspann.it;

import com.fspann.api.ForwardSecureANNSystem;
import com.fspann.common.*;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.key.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;

import javax.crypto.SecretKey;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ForwardSecurityRotationIT {

    @Disabled
    @Test
    void testOldKeyCannotDecryptAfterRotation() throws Exception {
        Path root = Files.createTempDirectory("fspann-rot");
        Path meta = root.resolve("meta");
        Path pts  = root.resolve("pts");
        Path keys = root.resolve("keys");

        Files.createDirectories(meta);
        Files.createDirectories(pts);
        Files.createDirectories(keys);

        RocksDBMetadataManager metadata =
                RocksDBMetadataManager.create(meta.toString(), pts.toString());

        Path cfgFile = root.resolve("cfg.json");
        Files.writeString(cfgFile, """
        {
          "paper": { 
            "enabled": true, 
            "divisions": 3, 
            "m": 8, 
            "lambda": 2,
            "seed": 7 
          },
          "partitionedIndexingEnabled": true,
          "reencryptionEnabled": true,
          "output": { "exportArtifacts": false }
        }
        """);

        KeyManager km = new KeyManager(keys.resolve("ks.blob").toString());
        KeyRotationServiceImpl keyService =
                new KeyRotationServiceImpl(
                        km,
                        new KeyRotationPolicy(100000, Long.MAX_VALUE),
                        meta.toString(),
                        metadata,
                        null
                );

        AesGcmCryptoService crypto =
                new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadata);
        keyService.setCryptoService(crypto);

        ForwardSecureANNSystem system = new ForwardSecureANNSystem(
                cfgFile.toString(),
                root.resolve("seed.csv").toString(),
                keys.toString(),
                List.of(3),
                root,
                false,
                metadata,
                crypto,
                128
        );

        // FIXED: Use batchInsert instead of individual insert
        system.batchInsert(List.of(
                new double[]{1, 2, 3},
                new double[]{4, 5, 6},
                new double[]{7, 8, 9}
        ), 3);

        system.finalizeForSearch();
        system.flushAll();
        metadata.flush();
        Thread.sleep(100);  // Extra safety

        // Verify point exists (use "0" as batchInsert uses numeric IDs)
        String pointId = "0";
        EncryptedPoint before = metadata.loadEncryptedPoint(pointId);
        assertNotNull(before, "Encrypted point must exist after finalize");

        int oldVersion = keyService.getCurrentVersion().getVersion();
        SecretKey oldKey = keyService.getCurrentVersion().getKey();

        // Rotate and re-encrypt
        keyService.rotateKeyOnly();
        keyService.initializeUsageTracking();
        keyService.reEncryptAll();

        system.flushAll();
        metadata.flush();
        Thread.sleep(100);

        // Load re-encrypted point
        EncryptedPoint after = metadata.loadEncryptedPoint(pointId);
        assertNotNull(after, "Point should exist after re-encryption");
        assertEquals(oldVersion + 1, after.getVersion(),
                "Re-encrypted point should have new version");

        // Verify old key cannot decrypt
        assertThrows(
                RuntimeException.class,
                () -> crypto.decryptFromPoint(after, oldKey),
                "Old key should not be able to decrypt re-encrypted point"
        );

        // But current key should work
        double[] vec = crypto.decryptFromPoint(after, null);
        assertEquals(3, vec.length);

        system.shutdown();
        metadata.close();
    }
}