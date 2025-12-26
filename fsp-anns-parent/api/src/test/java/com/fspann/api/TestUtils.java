package com.fspann.api;

import com.fspann.common.RocksDBMetadataManager;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.crypto.CryptoService;
import com.fspann.key.KeyManager;
import com.fspann.key.KeyRotationPolicy;
import com.fspann.key.KeyRotationServiceImpl;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

final class TestUtils {

    static void writeIvecs(Path out, int[][] rows) throws IOException {
        try (DataOutputStream dos =
                     new DataOutputStream(Files.newOutputStream(out))) {
            for (int[] r : rows) {
                dos.writeInt(Integer.reverseBytes(r.length));
                for (int v : r) {
                    dos.writeInt(Integer.reverseBytes(v));
                }
            }
        }
    }

    static RocksDBMetadataManager mockMetadata(Path tmp) throws IOException {
        Path meta = tmp.resolve("metadata");
        Path pts  = tmp.resolve("points");
        Files.createDirectories(meta);
        Files.createDirectories(pts);

        return RocksDBMetadataManager.create(
                meta.toString(),
                pts.toString()
        );
    }

    static CryptoService mockCrypto(Path tmp) throws IOException {

        Path ks = tmp.resolve("keys");
        Files.createDirectories(ks);

        KeyManager km = new KeyManager(ks.toString());

        KeyRotationServiceImpl keyService =
                new KeyRotationServiceImpl(
                        km,
                        new KeyRotationPolicy(Integer.MAX_VALUE, Long.MAX_VALUE),
                        tmp.resolve("metadata").toString(),
                        null,
                        null
                );

        CryptoService crypto =
                new AesGcmCryptoService(
                        new SimpleMeterRegistry(),
                        keyService,
                        null
                );

        keyService.setCryptoService(crypto);
        return crypto;
    }

    static ForwardSecureANNSystem minimalSystem(Path tmp) throws Exception {

        // minimal config file
        Path cfg = tmp.resolve("test-config.json");
        Files.writeString(cfg, """
        {
          "paper": { "m": 4, "lambda": 2, "divisions": 1 },
          "eval": { "kVariants": [1,5] }
        }
        """);

        RocksDBMetadataManager md = mockMetadata(tmp);
        CryptoService crypto = mockCrypto(tmp);

        return new ForwardSecureANNSystem(
                cfg.toString(),
                "POINTS_ONLY",
                tmp.resolve("keys").toString(),
                List.of(2),
                tmp,
                false,
                md,
                crypto,
                10
        );
    }

    private TestUtils() {}
}
