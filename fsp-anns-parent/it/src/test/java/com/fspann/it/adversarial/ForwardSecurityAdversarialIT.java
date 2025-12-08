package com.fspann.it.adversarial;

import com.fspann.api.ForwardSecureANNSystem;
import com.fspann.common.EncryptedPoint;
import com.fspann.common.RocksDBMetadataManager;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.key.KeyManager;
import com.fspann.key.KeyRotationPolicy;
import com.fspann.key.KeyRotationServiceImpl;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

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
        Path pointsDir = temp.resolve("points");

        Files.createDirectories(keysDir);
        Files.createDirectories(metaDir);
        Files.createDirectories(pointsDir);

        meta = RocksDBMetadataManager.create(metaDir.toString(), pointsDir.toString());

        KeyManager km = new KeyManager(keysDir.resolve("ks.blob").toString());
        keySvc = new KeyRotationServiceImpl(
                km,
                new KeyRotationPolicy(100_000, 100_000),
                metaDir.toString(),
                meta,
                null
        );

        crypto = new AesGcmCryptoService(new SimpleMeterRegistry(), keySvc, meta);
        keySvc.setCryptoService(crypto);
        String safePath = temp.toString().replace("\\", "\\\\");

        Path cfg = temp.resolve("conf.json");
        Files.writeString(cfg, """
{
  "numShards": 1,
  "profilerEnabled": false,
  "lsh": { "numTables": 1, "rowsPerBand": 2, "probeShards": 1 },
  "ratio": { "source": "base" },
  "output": { "resultsDir": "%s", "exportArtifacts": false }
}
""".formatted(safePath));

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

        keySvc.rotateKeyOnly();   // correct method
        keyAfterRotate = keySvc.getCurrentVersion().getKey();

        assertNotEquals(compromisedKeyBefore, keyAfterRotate);
    }


    // -------------------------------------------------------------------------
    //  GAME 1 — Old key must NOT decrypt new ciphertext after full re-encryption
    // -------------------------------------------------------------------------
    @Test
    void compromisedKeyCannotDecryptAfterRotation() {

        keySvc.reEncryptAll(); // full forward-secure rewrite

        int success = 0;

        for (EncryptedPoint p : meta.getAllEncryptedPoints()) {
            try {
                crypto.decryptFromPoint(p, compromisedKeyBefore);
                success++;
            } catch (Exception ignore) {}
        }

        assertEquals(0, success, "Old key MUST NOT decrypt new ciphertext");
    }

    // -------------------------------------------------------------------------
    //  GAME 2 — Re-encryption must change ciphertext on disk
    // -------------------------------------------------------------------------
    @Test
    void reencryptAllChangesCiphertext() {

        Map<String, byte[]> before = snapshotCiphertext();

        keySvc.reEncryptAll();

        Map<String, byte[]> after = snapshotCiphertext();

        assertNotEquals(before, after, "Ciphertext files must be modified by reEncryptAll()");
    }

    // -------------------------------------------------------------------------
    //  GAME 3 — Selective re-encryption must rewrite only touched IDs
    // -------------------------------------------------------------------------
    @Test
    void selectiveReencryptionOnlyUpdatesTouched(){

        List<String> ids = meta.getAllVectorIds();
        assertFalse(ids.isEmpty());

        var touched = List.of(ids.get(0));

        Map<String, byte[]> before = snapshotCiphertext();

        var rep = keySvc.reencryptTouched(touched, keySvc.getCurrentVersion().getVersion(), () -> meta.sizePointsDir());
        assertTrue(rep.reencryptedCount() > 0);

        Map<String, byte[]> after = snapshotCiphertext();

        assertNotEquals(before.get(touched.get(0)), after.get(touched.get(0)),
                "Touched vector must be rewritten");

        for (int i = 1; i < ids.size(); i++) {
            assertArrayEquals(before.get(ids.get(i)), after.get(ids.get(i)),
                    "Untouched vectors MUST remain identical");
        }
    }

    // Utility to snapshot ciphertexts from disk
    private Map<String, byte[]> snapshotCiphertext(){
        Map<String, byte[]> out = new HashMap<>();
        for (EncryptedPoint p : meta.getAllEncryptedPoints()) {
            out.put(p.getId(), Arrays.copyOf(p.getCiphertext(), p.getCiphertext().length));
        }
        return out;
    }
}
