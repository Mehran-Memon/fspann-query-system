package com.fspann.it.adversarial;

import com.fspann.api.ForwardSecureANNSystem;
import com.fspann.common.EncryptedPoint;
import com.fspann.common.RocksDBMetadataManager;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.key.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import javax.crypto.SecretKey;
import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ForwardSecurityAdversarialIT {

    @TempDir
    Path temp;

    ForwardSecureANNSystem sys;
    RocksDBMetadataManager meta;
    KeyRotationServiceImpl keySvc;
    AesGcmCryptoService crypto;

    SecretKey compromisedKeyBefore;
    SecretKey keyAfterRotate;

    @BeforeEach
    void init() throws Exception {

        Path keysDir   = temp.resolve("keys");
        Path metaDir   = temp.resolve("meta");
        Path ptsDir    = temp.resolve("pts");

        Files.createDirectories(keysDir);
        Files.createDirectories(metaDir);
        Files.createDirectories(ptsDir);

        meta = RocksDBMetadataManager.create(metaDir.toString(), ptsDir.toString());

        KeyManager km = new KeyManager(keysDir.resolve("ks.blob").toString());
        keySvc = new KeyRotationServiceImpl(
                km,
                new KeyRotationPolicy(100000, 100000),
                metaDir.toString(),
                meta,
                null
        );

        crypto = new AesGcmCryptoService(new SimpleMeterRegistry(), keySvc, meta);
        keySvc.setCryptoService(crypto);

        Path cfg = temp.resolve("cfg.json");
        Files.writeString(cfg, """
        {
          "paper": { "enabled": true, "m": 3, "divisions": 3, "lambda": 3, "seed": 13 },
          "lsh": { "numTables": 1, "rowsPerBand": 2, "probeShards": 1 },
          "ratio": { "source": "base" },
          "output": { "exportArtifacts": false },
          "reencryption": { "enabled": true }
        }
        """);

        sys = new ForwardSecureANNSystem(
                cfg.toString(),
                temp.resolve("seed.csv").toString(),
                keysDir.toString(),
                List.of(2),
                temp,
                false,
                meta,
                crypto,
                128
        );

        sys.batchInsert(List.of(
                new double[]{0,0},
                new double[]{0.1,0.1},
                new double[]{0.2,0.2}
        ), 2);

        sys.flushAll();
        sys.finalizeForSearch();

        compromisedKeyBefore = keySvc.getCurrentVersion().getKey();
        keySvc.rotateKeyOnly();
        keyAfterRotate = keySvc.getCurrentVersion().getKey();

        assertNotEquals(compromisedKeyBefore, keyAfterRotate);
    }

    @Test
    void compromisedKeyCannotDecryptAfterRotation() {
        keySvc.reEncryptAll();
        int ok = 0;
        for (EncryptedPoint p : meta.getAllEncryptedPoints()) {
            try {
                crypto.decryptFromPoint(p, compromisedKeyBefore);
                ok++;
            } catch (Exception ignore) {}
        }
        assertEquals(0, ok);
    }

    @Test
    void reencryptAllChangesCiphertext() {
        var before = snapshot();
        keySvc.reEncryptAll();
        var after = snapshot();
        assertNotEquals(before, after);
    }

    @Test
    void selectiveReencryptionOnlyUpdatesTouched() {
        List<String> ids = meta.getAllVectorIds();
        assertFalse(ids.isEmpty());

        var touched = List.of(ids.get(0));

        var before = snapshot();
        var rep = keySvc.reencryptTouched(touched, keySvc.getCurrentVersion().getVersion(), () -> meta.sizePointsDir());
        assertTrue(rep.reencryptedCount() > 0);

        var after = snapshot();

        assertNotEquals(before.get(touched.get(0)), after.get(touched.get(0)));

        for (int i = 1; i < ids.size(); i++)
            assertArrayEquals(before.get(ids.get(i)), after.get(ids.get(i)));
    }

    @AfterEach
    void cleanup() {
        try { sys.setExitOnShutdown(false); sys.shutdown(); } catch (Exception ignore) {}
        try { meta.close(); } catch (Exception ignore) {}
    }

    private Map<String, byte[]> snapshot() {
        Map<String,byte[]> m = new HashMap<>();
        for (EncryptedPoint p : meta.getAllEncryptedPoints())
            m.put(p.getId(), Arrays.copyOf(p.getCiphertext(), p.getCiphertext().length));
        return m;
    }
}
