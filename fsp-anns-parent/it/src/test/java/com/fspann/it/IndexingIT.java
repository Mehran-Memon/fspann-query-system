package com.fspann.it;

import com.fspann.api.ForwardSecureANNSystem;
import com.fspann.api.ApiSystemConfig;
import com.fspann.config.SystemConfig;

import com.fspann.common.*;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.key.*;

import org.junit.jupiter.api.*;

import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class IndexingIT extends BaseSystemIT {

    static Path root;
    static Path metaDir, ptsDir, ksFile, seedFile, cfgFile;

    static RocksDBMetadataManager metadata;
    static KeyManager km;
    static KeyRotationServiceImpl keyService;
    static AesGcmCryptoService crypto;
    static SystemConfig cfg;

    @BeforeAll
    static void init() throws Exception {

        root    = Files.createTempDirectory("idx_it_");
        metaDir = root.resolve("metadata");
        ptsDir  = root.resolve("points");
        ksFile  = root.resolve("keystore.bin");
        seedFile = root.resolve("seed.csv");

        Files.createDirectories(metaDir);
        Files.createDirectories(ptsDir);

        Files.writeString(seedFile, "");

        cfgFile = Files.createTempFile(root, "cfg_", ".json");
        Files.writeString(cfgFile, """
        {
          "paper": { "enabled": true, "m": 3, "divisions": 3, "lambda": 3, "seed": 13 },
          "lsh":   { "numTables": 0, "rowsPerBand": 0, "probeShards": 0 },
          "eval":  { "computePrecision": false },
          "ratio": { "source": "base" },
          "reencryption": { "enabled": false },
          "output": { "exportArtifacts": false },
          "numShards": 8,
          "opsThreshold": 9999999,
          "ageThresholdMs": 99999999
        }
        """);

        cfg = new ApiSystemConfig(cfgFile.toString()).getConfig();

        metadata = RocksDBMetadataManager.create(metaDir.toString(), ptsDir.toString());

        km = new KeyManager(ksFile.toString());
        KeyRotationPolicy pol = new KeyRotationPolicy(
                (int) cfg.getOpsThreshold(), cfg.getAgeThresholdMs());

        keyService = new KeyRotationServiceImpl(
                km, pol, metaDir.toString(), metadata, null);

        crypto = new AesGcmCryptoService(null, keyService, metadata);
        keyService.setCryptoService(crypto);
    }

    @Test
    @Order(1)
    void testBasicIndexing() throws Exception {

        ForwardSecureANNSystem sys = new ForwardSecureANNSystem(
                cfgFile.toString(), seedFile.toString(), ksFile.toString(),
                List.of(8), root, false, metadata, crypto, 32
        );

        keyService.setIndexService(sys.getIndexService());

        Random r = new Random(123);
        List<double[]> vs = new ArrayList<>();

        for (int i = 0; i < 64; i++) {
            double[] v = new double[8];
            for (int j = 0; j < 8; j++)
                v[j] = r.nextDouble();
            vs.add(v);
        }

        sys.batchInsert(vs, 8);
        assertEquals(64, sys.getIndexedVectorCount());

        int stored = metadata.getAllEncryptedPoints().size();
        assertEquals(64, stored);

        sys.setExitOnShutdown(false);
    }

    @Test
    @Order(2)
    void testFinalizeForSearch() throws Exception {

        ForwardSecureANNSystem sys = new ForwardSecureANNSystem(
                cfgFile.toString(), seedFile.toString(), ksFile.toString(),
                List.of(8), root, false, metadata, crypto, 32
        );

        keyService.setIndexService(sys.getIndexService());

        Random r = new Random(99);
        List<double[]> vs = new ArrayList<>();

        for (int i = 0; i < 16; i++) {
            double[] v = new double[8];
            for (int j = 0; j < 8; j++)
                v[j] = r.nextDouble();
            vs.add(v);
        }
        sys.batchInsert(vs, 8);

        assertDoesNotThrow(sys::finalizeForSearch);

        sys.setExitOnShutdown(false);
    }

    @AfterAll
    static void cleanup() throws Exception {
        try { metadata.close(); } catch (Exception ignore) {}
        if (root != null)
            Files.walk(root).sorted(Comparator.reverseOrder())
                    .forEach(p -> { try { Files.deleteIfExists(p); } catch (Exception ignore) {} });
    }
}