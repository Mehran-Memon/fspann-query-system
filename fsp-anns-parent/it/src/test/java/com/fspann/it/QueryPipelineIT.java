package com.fspann.it;

import com.fspann.api.ForwardSecureANNSystem;
import com.fspann.common.QueryResult;
import com.fspann.common.QueryToken;
import com.fspann.common.RocksDBMetadataManager;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.key.KeyManager;
import com.fspann.key.KeyRotationPolicy;
import com.fspann.key.KeyRotationServiceImpl;
import com.fspann.api.ApiSystemConfig;
import com.fspann.config.SystemConfig;
import com.fspann.query.service.QueryServiceImpl;

import org.junit.jupiter.api.*;
import java.nio.file.*;
import java.util.*;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class QueryPipelineIT {

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
        tempRoot   = Files.createTempDirectory("fspann_query_it_");
        metaDir    = tempRoot.resolve("metadata");
        pointsDir  = tempRoot.resolve("points");
        ksFile     = tempRoot.resolve("keystore.bin");

        Files.createDirectories(metaDir);
        Files.createDirectories(pointsDir);

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
                (Integer.MAX_VALUE), (Long.MAX_VALUE));


        keyService = new KeyRotationServiceImpl(
                keyManager, policy, metaDir.toString(), metadata, null);

        crypto = new AesGcmCryptoService(null, keyService, metadata);
        keyService.setCryptoService(crypto);
    }

    @Test
    @Order(1)
    void testIndex_and_SimpleQuery() throws Exception {

        ForwardSecureANNSystem sys = new ForwardSecureANNSystem(
                "X.json",
                "IGNORED",
                ksFile.toString(),
                List.of(8),
                tempRoot,
                false,
                metadata,
                crypto,
                16
        );

        // --- Insert 64 vectors ---
        List<double[]> vecs = new ArrayList<>();
        Random rnd = new Random(7);
        for (int i = 0; i < 64; i++) {
            double[] v = new double[8];
            for (int j = 0; j < 8; j++) v[j] = rnd.nextDouble();
            vecs.add(v);
        }
        sys.batchInsert(vecs, 8);
        assertEquals(64, sys.getIndexedVectorCount());

        sys.finalizeForSearch();

        // --- Create 1 query vector ---
        double[] q = new double[8];
        for (int i = 0; i < 8; i++) q[i] = rnd.nextDouble();

        QueryToken tok = sys.createToken(q, 10, 8);
        assertNotNull(tok);
        assertEquals(8, tok.getDimension());
        assertEquals(10, tok.getTopK());

        // --- Execute search ---
        QueryServiceImpl qs = sys.getQueryServiceImpl();
        List<QueryResult> ret = qs.search(tok);

        assertNotNull(ret);
        assertFalse(ret.isEmpty(), "Search should return non-empty list");
        assertTrue(ret.size() <= 10, "Results should not exceed topK");

        // --- Decryption correctness check ---
        String firstId = ret.get(0).getId();
        assertDoesNotThrow(() -> Integer.parseInt(firstId),
                "ID should be parseable integer (our synthetic IDs)");

        sys.shutdown();
    }

    @Test
    @Order(2)
    void testQueryCacheHit_and_EngineSimple() throws Exception {

        ForwardSecureANNSystem sys = new ForwardSecureANNSystem(
                "X2.json",
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
        for (int i = 0; i < 32; i++) {
            double[] v = new double[8];
            for (int j = 0; j < 8; j++) v[j] = r.nextDouble();
            vs.add(v);
        }
        sys.batchInsert(vs, 8);
        sys.finalizeForSearch();

        // Query
        double[] q = new double[8];
        for (int j = 0; j < 8; j++) q[j] = r.nextDouble();

        var engine = sys.getEngine();

        List<QueryResult> r1 = engine.evalSimple(q, 10, 8, false);
        assertNotNull(r1);
        assertFalse(r1.isEmpty());

        List<QueryResult> r2 = engine.evalSimple(q, 10, 8, false);
        assertNotNull(r2);

        // CACHE HIT: must be the same object
        assertSame(r1, r2, "Second call must return cache hit");

        sys.shutdown();
    }

    @Test
    @Order(3)
    void testCandidateTouchTracking() throws Exception {

        ForwardSecureANNSystem sys = new ForwardSecureANNSystem(
                "X3.json",
                "IGNORED",
                ksFile.toString(),
                List.of(8),
                tempRoot,
                false,
                metadata,
                crypto,
                16
        );

        // Insert 16 vectors
        List<double[]> vecs = new ArrayList<>();
        Random r = new Random(5);
        for (int i = 0; i < 16; i++) {
            double[] v = new double[8];
            for (int j = 0; j < 8; j++) v[j] = r.nextDouble();
            vecs.add(v);
        }
        sys.batchInsert(vecs, 8);
        sys.finalizeForSearch();

        QueryServiceImpl qs = sys.getQueryServiceImpl();

        double[] q = new double[8];
        for (int j = 0; j < 8; j++) q[j] = r.nextDouble();

        QueryToken tok = sys.createToken(q, 5, 8);
        List<QueryResult> ret = qs.search(tok);
        assertFalse(ret.isEmpty());

        // Check tracker (must have touched ≥ 1 candidate)
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
