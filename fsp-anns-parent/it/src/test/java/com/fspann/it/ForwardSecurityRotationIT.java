package com.fspann.it;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.index.paper.PartitionedIndexService;
import com.fspann.key.*;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.crypto.SecretKey;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class ForwardSecurityRotationIT {

    private static final Logger logger =
            LoggerFactory.getLogger(ForwardSecurityRotationIT.class);

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

            try {
                metadata.flush();
            } catch (Exception e) {
                logger.warn("Flush after insert failed", e);
            }

            index.finalizeForSearch();
            metadata.flush();

            EncryptedPoint before = metadata.loadEncryptedPoint("p1");
            assertNotNull(before);


            int oldVersion = keyService.getCurrentVersion().getVersion();
            SecretKey oldKey = keyService.getCurrentVersion().getKey();

            // Rotate and re-encrypt
            keyService.rotateKeyOnly();
            keyService.reEncryptAll();
            metadata.flush();

            // ===== CRITICAL: Flush again after re-encryption =====
            try {
                metadata.flush();
            } catch (Exception e) {
                logger.warn("Flush after re-encryption failed", e);
            }

            EncryptedPoint after = metadata.loadEncryptedPoint("p1");
            assertNotNull(after, "Point should exist after re-encryption");
            assertEquals(oldVersion + 1, after.getVersion(),
                    "Re-encrypted point should have new version");

            // Now verify old key cannot decrypt
            assertThrows(
                    RuntimeException.class,
                    () -> crypto.decryptFromPoint(after, oldKey),
                    "Old key should not be able to decrypt re-encrypted point"
            );

            // But current key should work
            double[] vec = crypto.decryptFromPoint(after, null);
            assertEquals(3, vec.length);
        }
}
