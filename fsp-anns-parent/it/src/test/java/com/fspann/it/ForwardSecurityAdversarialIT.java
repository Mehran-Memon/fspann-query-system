package com.fspann.it;

import com.fspann.api.ForwardSecureANNSystem;
import com.fspann.common.*;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.key.*;
import com.fspann.api.ApiSystemConfig;
import com.fspann.config.SystemConfig;

import org.junit.jupiter.api.*;

import javax.crypto.SecretKey;
import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class ForwardSecurityAdversarialIT {

    Path root, meta, pts, ks;
    RocksDBMetadataManager metadata;
    KeyManager km;
    KeyRotationServiceImpl keyService;
    AesGcmCryptoService crypto;
    SystemConfig cfg;

    @BeforeEach
    void init() throws Exception {
        root = Files.createTempDirectory("fwdsec_");
        meta = root.resolve("metadata");
        pts  = root.resolve("points");
        ks   = root.resolve("keystore.bin");

        Files.createDirectories(meta);
        Files.createDirectories(pts);

        Path cfgFile = Files.createTempFile("cfg",".json");
        Files.writeString(cfgFile, """
        {
          "paper": {"enabled": true, "m":3, "divisions":3, "lambda":3, "seed":13},
          "lsh": {"numTables":0,"rowsPerBand":0,"probeShards":0},
          "reencryption": {"enabled": true},
          "opsThreshold": 10,
          "ageThresholdMs": 999999,
          "output": {"exportArtifacts": false}
        }
        """);
        cfg = new ApiSystemConfig(cfgFile.toString()).getConfig();

        metadata = RocksDBMetadataManager.create(meta.toString(), pts.toString());
        km = new KeyManager(ks.toString());
        KeyRotationPolicy pol = new KeyRotationPolicy(10, 999999);

        keyService = new KeyRotationServiceImpl(km, pol, meta.toString(), metadata, null);
        crypto = new AesGcmCryptoService(null, keyService, metadata);
        keyService.setCryptoService(crypto);
    }

    @Test
    void testKeyCompromiseForwardSecurity() throws Exception {

        ForwardSecureANNSystem sys = new ForwardSecureANNSystem(
                "CFG", "DATA", ks.toString(), List.of(4),
                root, false, metadata, crypto, 16
        );

        // --- Insert one vector ---
        double[] v = new double[]{0.1,0.2,0.3,0.4};
        sys.insert("x", v, 4);

        // --- Attacker obtains OLD key ---
        KeyVersion oldKv = keyService.getCurrentVersion();
        SecretKey oldKey = oldKv.getKey();

        // --- Encrypt/decrypt pre-rotation (attacker succeeds) ---
        EncryptedPoint ep = metadata.loadEncryptedPoint("x");
        double[] recovered = crypto.decryptFromPoint(ep, oldKey);
        assertNotNull(recovered);

        // --- Trigger rotation ---
        keyService.forceRotateNow();
        SecretKey newKey = keyService.getCurrentVersion().getKey();

        // --- Selective re-encrypt touched IDs ---
        var rep = keyService.reencryptTouched(List.of("x"),
                keyService.getCurrentVersion().getVersion(),
                () -> 0);

        assertEquals(1, rep.reencryptedCount());

        // --- Ciphertext changed ---
        EncryptedPoint ep2 = metadata.loadEncryptedPoint("x");
        assertNotEquals(
                Base64.getEncoder().encodeToString(ep.getCiphertext()),
                Base64.getEncoder().encodeToString(ep2.getCiphertext())
        );

        // --- Correct decryption with new key ---
        assertNotNull(crypto.decryptFromPoint(ep2, newKey));

        // --- OLD KEY MUST FAIL NOW ---
        assertNull(crypto.decryptFromPoint(ep2, oldKey),
                "Forward security violated: old key should NOT decrypt new ciphertext");
    }
}
