package com.fspann.it.adversarial;

import com.fspann.api.ForwardSecureANNSystem;
import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.index.paper.GFunctionRegistry;
import com.fspann.key.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import javax.crypto.SecretKey;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;


@Disabled("TODO: Fix resource leak")
@Execution(ExecutionMode.SAME_THREAD)
@Timeout(value = 10, unit = TimeUnit.SECONDS)
class ForwardSecurityGame_G6_Test {

    @TempDir
    Path temp;

    ForwardSecureANNSystem system;
    RocksDBMetadataManager metadata;
    KeyRotationServiceImpl keyService;
    AesGcmCryptoService crypto;

    SecretKey compromisedKey;
    int compromisedVersion;

    List<EncryptedPoint> snapshotPoints;

    @BeforeEach
    void setup() throws Exception {
        Path metaDir = temp.resolve("meta");
        Path ptsDir  = temp.resolve("pts");
        Path keyDir  = temp.resolve("keys");
        Files.createDirectories(metaDir);
        Files.createDirectories(ptsDir);
        Files.createDirectories(keyDir);

        Path seedFile = temp.resolve("seed.csv");
        Files.writeString(seedFile, "");

        metadata = RocksDBMetadataManager.create(
                metaDir.toString(),
                ptsDir.toString()
        );

        Path cfg = temp.resolve("cfg.json");
        Files.writeString(cfg, """
        {
          "paper": {
            "enabled": true,
            "tables": 2,
            "divisions": 2,
            "m": 4,
            "lambda": 3,
            "seed": 13
          },
          "reencryption": { "enabled": true }
        }
        """);

        SystemConfig sc = SystemConfig.load(cfg.toString(), true);
        GFunctionRegistry.reset();
        GFunctionRegistry.initialize(
                List.of(
                        new double[]{1,2,3,4},
                        new double[]{2,3,4,5}
                ),
                4,
                sc.getPaper().getM(),
                sc.getPaper().getLambda(),
                sc.getPaper().getSeed(),
                sc.getPaper().getTables(),
                sc.getPaper().getDivisions()
        );

        KeyManager km = new KeyManager(keyDir.resolve("ks.blob").toString());
        keyService = new KeyRotationServiceImpl(
                km,
                new KeyRotationPolicy(1, Long.MAX_VALUE),
                metaDir.toString(),
                metadata,
                null
        );

        crypto = new AesGcmCryptoService(
                new SimpleMeterRegistry(),
                keyService,
                metadata
        );
        keyService.setCryptoService(crypto);

        system = new ForwardSecureANNSystem(
                cfg.toString(),
                seedFile.toString(),
                keyDir.toString(),
                List.of(4),
                temp,
                false,
                metadata,
                crypto,
                32
        );

        for (int i = 0; i < 6; i++) {
            system.insert(
                    "p" + i,
                    new double[]{i, i+1, i+2, i+3},
                    4
            );
        }

        system.finalizeForSearch();

        compromisedKey = keyService.getCurrentVersion().getKey();
        compromisedVersion = keyService.getCurrentVersion().getVersion();
        snapshotPoints = new ArrayList<>(metadata.getAllEncryptedPoints());
    }

    /* ===================== G₆ ===================== */
    @Test
    void queryCorrectnessUnderRotation() {
        double[] query = {1.5, 2.5, 3.5, 4.5};

        QueryToken t1 = system.createToken(query, 1, 4);
        List<QueryResult> before = system.getQueryServiceImpl().search(t1);

        keyService.rotateKeyOnly();
        keyService.initializeUsageTracking();
        keyService.reEncryptAll();

        QueryToken t2 = system.createToken(query, 1, 4);
        List<QueryResult> after = system.getQueryServiceImpl().search(t2);

        if (!before.isEmpty()) {
            assertFalse(after.isEmpty());
            assertEquals(before.get(0).getId(), after.get(0).getId());
        } else {
            assertTrue(after.isEmpty());
        }
    }

    @AfterEach
    void tearDown() {
        if (system != null) {
            system.shutdownForTests();
        }
    }
}