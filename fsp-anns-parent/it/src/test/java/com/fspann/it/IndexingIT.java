package com.fspann.it;

import com.fspann.api.ForwardSecureANNSystem;
import com.fspann.common.RocksDBMetadataManager;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.key.KeyManager;
import com.fspann.key.KeyRotationPolicy;
import com.fspann.key.KeyRotationServiceImpl;
import com.fspann.config.SystemConfig;
import com.fspann.api.ApiSystemConfig;

import org.junit.jupiter.api.*;
import java.nio.file.*;
import java.util.*;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class IndexingIT {

    static Path tempRoot;
    static Path metaDir;
    static Path pointsDir;
    static Path ksFile;
    static RocksDBMetadataManager metadata;
    static KeyManager keyManager;
    static KeyRotationServiceImpl keyService;
    static AesGcmCryptoService crypto;

    static SystemConfig cfg;

    @BeforeAll
    static void init() throws Exception {

        tempRoot   = Files.createTempDirectory("fspann_index_it_");
        metaDir    = tempRoot.resolve("metadata");
        pointsDir  = tempRoot.resolve("points");
        ksFile     = tempRoot.resolve("keystore.bin");

        Files.createDirectories(metaDir);
        Files.createDirectories(pointsDir);

        // Load minimal config (Option-C)
        Path cfgPath = Files.createTempFile("optc_cfg_", ".json");
        Files.writeString(cfgPath, """
        {
          "paper": { "enabled": true, "m": 3, "divisions": 3, "lambda": 3, "seed": 13 },
          "lsh":   { "numTables": 0, "rowsPerBand": 0, "probeShards": 0 },
          "eval":  { "computePrecision": false, "writeGlobalPrecisionCsv": false,
                     "kVariants": [1,5,10] },
          "ratio": { "source": "base" },
          "opsThreshold": 999999,
          "ageThresholdMs": 999999,
          "numShards": 8,
          "output": { "exportArtifacts": false, "resultsDir": "out" },
          "reencryption": { "enabled": false }
        }
        """);

        cfg = new ApiSystemConfig(cfgPath.toString()).getConfig();

        metadata = RocksDBMetadataManager.create(
                metaDir.toString(), pointsDir.toString());

        keyManager = new KeyManager(ksFile.toString());
        KeyRotationPolicy policy = new KeyRotationPolicy(
                (int) cfg.getOpsThreshold(), cfg.getAgeThresholdMs());

        keyService = new KeyRotationServiceImpl(
                keyManager, policy, metaDir.toString(), metadata, null);

        crypto = new AesGcmCryptoService(null, keyService, metadata);
        keyService.setCryptoService(crypto);
    }

    @Test
    @Order(1)
    void testBasicIndexing_smallDim8() throws Exception {

        ForwardSecureANNSystem sys = new ForwardSecureANNSystem(
                "ignored_config_path.json",
                "IGNORED",
                ksFile.toString(),
                List.of(8),          // dim
                tempRoot,
                false,
                metadata,
                crypto,
                16                   // batchSize
        );

        // Create 64 small vectors (dim=8)
        List<double[]> vecs = new ArrayList<>();
        Random r = new Random(123);
        for (int i = 0; i < 64; i++) {
            double[] v = new double[8];
            for (int j = 0; j < 8; j++) v[j] = r.nextDouble();
            vecs.add(v);
        }

        // Insert in two passes of 32 sized chunks
        sys.batchInsert(vecs.subList(0, 32), 8);
        sys.batchInsert(vecs.subList(32, 64), 8);

        assertEquals(64, sys.getIndexedVectorCount(), "Total inserted mismatch");

        // Ensure encrypted points exist in RocksDB
        int stored = metadata.getAllEncryptedPoints().size();
        assertEquals(64, stored, "Metadata DB should store all encrypted points");

        // Ensure a key version exists
        assertTrue(keyService.getCurrentVersion().getVersion() >= 1,
                "Key version must be >=1");

        // Shutdown cleanly
        assertDoesNotThrow(sys::shutdown);
    }

    @Test
    @Order(2)
    void testIndex_thenFinalizeForSearch() throws Exception {

        ForwardSecureANNSystem sys = new ForwardSecureANNSystem(
                "ignored_config_path.json",
                "IGNORED",
                ksFile.toString(),
                List.of(8),
                tempRoot,
                false,
                metadata,
                crypto,
                16
        );

        // Insert small batch
        List<double[]> vs = new ArrayList<>();
        Random r = new Random(99);
        for (int i = 0; i < 16; i++) {
            double[] v = new double[8];
            for (int j = 0; j < 8; j++) v[j] = r.nextDouble();
            vs.add(v);
        }
        sys.batchInsert(vs, 8);

        // finalize does flush + disable write-through + optimize
        assertDoesNotThrow(sys::finalizeForSearch);

        sys.shutdown();
    }

    @AfterAll
    static void cleanup() throws IOException {
        try { metadata.close(); } catch (Exception ignore) {}
        Files.walk(tempRoot)
                .sorted(Comparator.reverseOrder())
                .forEach(p -> {
                    try { Files.deleteIfExists(p); }
                    catch (Exception ignore) {}
                });
    }
}
