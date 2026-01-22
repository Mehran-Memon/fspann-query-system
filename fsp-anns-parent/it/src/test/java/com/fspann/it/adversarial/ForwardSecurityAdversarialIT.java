package com.fspann.it.adversarial;

import com.fspann.api.ForwardSecureANNSystem;
import com.fspann.common.EncryptedPoint;
import com.fspann.common.QueryToken;
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

    private static final int DIM = 3;  // Use consistent dimension

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
          "paper": { 
            "enabled": true, 
            "m": 3, 
            "divisions": 3, 
            "lambda": 3, 
            "seed": 13 
          },
          "lsh": { "numTables": 1, "rowsPerBand": 2, "probeShards": 1 },
          "ratio": { "source": "base" },
          "output": { "exportArtifacts": false },
          "reencryption": { "enabled": true },
          "stabilization": {
            "enabled": true,
            "alpha": 0.5,
            "minCandidatesRatio": 1.0
          }
        }
        """);

        sys = new ForwardSecureANNSystem(
                cfg.toString(),
                temp.resolve("seed.csv").toString(),
                keysDir.toString(),
                List.of(DIM),  // FIXED: Use DIM=3
                temp,
                false,
                meta,
                crypto,
                128
        );

        // Insert 3D vectors
        sys.batchInsert(List.of(
                new double[]{0.0, 0.0, 0.0},
                new double[]{0.1, 0.1, 0.1},
                new double[]{0.2, 0.2, 0.2}
        ), DIM);

        sys.flushAll();
        sys.finalizeForSearch();

        compromisedKeyBefore = keySvc.getCurrentVersion().getKey();
        keySvc.rotateKeyOnly();
        keyAfterRotate = keySvc.getCurrentVersion().getKey();

        assertNotEquals(compromisedKeyBefore, keyAfterRotate);
    }

    @Test
    void compromisedKeyCannotDecryptAfterRotation() {
        keySvc.initializeUsageTracking();
        keySvc.initializeUsageTracking();
        keySvc.reencryptTouched(
                meta.getAllVectorIds(),
                keySvc.getCurrentVersion().getVersion(),
                () -> 0L
        );

        int ok = 0;
        for (EncryptedPoint p : meta.getAllEncryptedPoints()) {
            try {
                crypto.decryptFromPoint(p, compromisedKeyBefore);
                ok++;
            } catch (Exception ignored) {}
        }

        assertEquals(0, ok, "Old key must not decrypt any re-encrypted point");
    }

    @Test
    void reencryptAllChangesCiphertext() {
        var before = snapshot();
        keySvc.initializeUsageTracking();
        keySvc.initializeUsageTracking();
        keySvc.reencryptTouched(
                meta.getAllVectorIds(),
                keySvc.getCurrentVersion().getVersion(),
                () -> 0L
        );
        var after = snapshot();
        assertNotEquals(before, after);
    }

    @Test
    void selectiveReencryptionOnlyUpdatesTouched() {
        List<String> ids = meta.getAllVectorIds();
        assertFalse(ids.isEmpty());

        var touched = List.of(ids.get(0));

        var before = snapshot();
        keySvc.initializeUsageTracking();
        var rep = keySvc.reencryptTouched(
                touched,
                keySvc.getCurrentVersion().getVersion(),
                () -> meta.sizePointsDir()
        );
        assertTrue(rep.reencryptedCount() > 0);

        var after = snapshot();

        assertNotEquals(before.get(touched.get(0)), after.get(touched.get(0)));

        for (int i = 1; i < ids.size(); i++)
            assertArrayEquals(before.get(ids.get(i)), after.get(ids.get(i)));
    }

    @Test
    void keyTrackingPreventsUnsafeDeletion() {

        // Rebuild usage tracking from metadata
        keySvc.initializeUsageTracking();

        KeyManager km = keySvc.getKeyManager();
        KeyUsageTracker tracker = km.getUsageTracker();

        int current = keySvc.getCurrentVersion().getVersion();
        int previous = current - 1;

        System.out.println("=== KEY TRACKING STATE ===");
        System.out.println("Current key: v" + current);
        System.out.println("Previous key: v" + previous);
        System.out.println(tracker.getSummary());

        assertEquals(3, tracker.getVectorCount(previous),
                "All vectors must remain on previous key after rotateKeyOnly()");
        assertEquals(0, tracker.getVectorCount(current),
                "No vectors should use the new key before re-encryption");

        assertFalse(tracker.isSafeToDelete(previous),
                "Previous key must NOT be deletable while vectors still use it");

        km.deleteKeysOlderThan(previous + 1);
        assertNotNull(km.getSessionKey(previous),
                "Previous key must not be deleted while vectors exist");

        keySvc.initializeUsageTracking();
        keySvc.reencryptTouched(
                meta.getAllVectorIds(),
                keySvc.getCurrentVersion().getVersion(),
                () -> 0L
        );

        assertEquals(0, tracker.getVectorCount(previous),
                "Previous key must have zero vectors after re-encryption");
        assertEquals(3, tracker.getVectorCount(current),
                "All vectors must migrate to current key after re-encryption");

        assertTrue(tracker.isSafeToDelete(previous),
                "Previous key must be safe to delete after migration");

        km.deleteKeysOlderThan(previous + 1);
        assertNull(km.getSessionKey(previous),
                "Previous key must be deleted after all vectors migrate");
    }

    @Test
    void selectiveReencryptionMigratesOnlyTouchedVectors() {

        // FIXED: Use 3D query matching DIM
        double[] q = {1.0, 1.0, 1.0};

        QueryToken tok = sys.createToken(q, 3, DIM);
        sys.getQueryServiceImpl().search(tok);

        int touched = sys.getIndexService().getLastTouchedCount();

        Assumptions.assumeTrue(
                touched > 0,
                "No vectors touched; selective re-encryption game undefined"
        );

        int oldVersion = keySvc.getCurrentVersion().getVersion();

        keySvc.rotateKeyOnly();
        keySvc.initializeUsageTracking();
        keySvc.reencryptTouched(
                new ArrayList<>(sys.getIndexService().getLastTouchedIds()),
                keySvc.getCurrentVersion().getVersion(),
                () -> meta.sizePointsDir()
        );

        int migrated = 0;
        for (EncryptedPoint p : meta.getAllEncryptedPoints()) {
            if (p.getVersion() > oldVersion) migrated++;
        }

        assertTrue(migrated > 0);
        assertTrue(migrated <= touched);
    }

    @AfterEach
    void cleanup() {
        try { sys.setExitOnShutdown(false); sys.shutdown(); } catch (Exception ignore) {}
        try { meta.close(); } catch (Exception ignore) {}
    }

    @AfterAll
    void shutdownOnce() {
        try { sys.shutdown(); } catch (Exception ignore) {}
        try { meta.close(); } catch (Exception ignore) {}
    }

    private Map<String, byte[]> snapshot() {
        Map<String,byte[]> m = new HashMap<>();
        for (EncryptedPoint p : meta.getAllEncryptedPoints())
            m.put(p.getId(), Arrays.copyOf(p.getCiphertext(), p.getCiphertext().length));
        return m;
    }
}