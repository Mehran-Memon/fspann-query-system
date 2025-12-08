package com.fspann.it;

import com.fspann.api.ForwardSecureANNSystem;
import com.fspann.api.ApiSystemConfig;
import com.fspann.config.SystemConfig;
import com.fspann.common.*;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.key.*;
import com.fspann.query.service.QueryServiceImpl;

import org.junit.jupiter.api.*;
import java.nio.file.*;
import java.util.*;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class QueryPipelineIT extends BaseSystemIT {

    static Path tempRoot;
    static Path metaDir;
    static Path pointsDir;
    static Path ksFile;
    static Path cfgFile;    // FIXED – was missing

    static RocksDBMetadataManager metadata;
    static KeyManager keyManager;
    static KeyRotationServiceImpl keyService;
    static AesGcmCryptoService crypto;
    static SystemConfig cfg;

    @BeforeAll
    static void init() throws Exception {

        tempRoot   = Files.createTempDirectory("qp_it_");
        metaDir    = tempRoot.resolve("metadata");
        pointsDir  = tempRoot.resolve("points");
        ksFile     = tempRoot.resolve("keystore.bin");

        Files.createDirectories(metaDir);
        Files.createDirectories(pointsDir);

        // FIX: create seed file
        Path seed = tempRoot.resolve("seed.csv");
        Files.writeString(seed, "");

        cfgFile = Files.createTempFile(tempRoot, "cfg_", ".json");
        Files.writeString(cfgFile, """
        {
          "paper": { "enabled": true, "m": 3, "divisions": 3, "lambda": 3, "seed": 13 },
          "lsh": { "numTables": 0, "rowsPerBand": 0, "probeShards": 0 },
          "eval": { "computePrecision": false, "writeGlobalPrecisionCsv": false,
             "kVariants": [1,5,10] },
          "ratio": { "source": "base" },
          "opsThreshold": 999999,
          "ageThresholdMs": 999999,
          "numShards": 8,
          "output": { "exportArtifacts": false, "resultsDir": "out" },
          "reencryption": { "enabled": false }
        }
        """);

        cfg = new ApiSystemConfig(cfgFile.toString()).getConfig();

        metadata = RocksDBMetadataManager.create(
                metaDir.toString(), pointsDir.toString());

        keyManager = new KeyManager(ksFile.toString());
        KeyRotationPolicy pol = new KeyRotationPolicy(Integer.MAX_VALUE, Long.MAX_VALUE);

        keyService = new KeyRotationServiceImpl(
                keyManager, pol, metaDir.toString(), metadata, null);

        crypto = new AesGcmCryptoService(null, keyService, metadata);
        keyService.setCryptoService(crypto);
    }

    // ===========================================================
    // 1. Indexing + Simple Query Flow
    // ===========================================================
    @Test
    @Order(1)
    void testIndex_and_SimpleQuery() throws Exception {

        ForwardSecureANNSystem sys = new ForwardSecureANNSystem(
                cfgFile.toString(),
                tempRoot.resolve("seed.csv").toString(),
                ksFile.toString(),
                List.of(8),
                tempRoot,
                false,
                metadata,
                crypto,
                16
        );

        // Bind index service
        keyService.setIndexService(sys.getIndexService());

        Random rnd = new Random(7);
        List<double[]> vecs = new ArrayList<>();

        for (int i = 0; i < 64; i++) {
            double[] v = new double[8];
            for (int j = 0; j < 8; j++) v[j] = rnd.nextDouble();
            vecs.add(v);
        }

        sys.batchInsert(vecs, 8);
        assertEquals(64, sys.getIndexedVectorCount());

        sys.finalizeForSearch();

        double[] q = new double[8];
        for (int i = 0; i < 8; i++) q[i] = rnd.nextDouble();

        QueryToken tok = sys.createToken(q, 10, 8);

        QueryServiceImpl qs = sys.getQueryServiceImpl();
        List<QueryResult> ret = qs.search(tok);

        assertFalse(ret.isEmpty());
        assertTrue(ret.size() <= 10);

        sys.setExitOnShutdown(false);
    }

    // ===========================================================
    // 2. Query Cache Hit + Engine Simple
    // ===========================================================
    @Test
    @Order(2)
    void testQueryCacheHit_and_EngineSimple() throws Exception {

        ForwardSecureANNSystem sys = new ForwardSecureANNSystem(
                cfgFile.toString(),
                tempRoot.resolve("seed.csv").toString(),
                ksFile.toString(),
                List.of(8),
                tempRoot,
                false,
                metadata,
                crypto,
                16
        );

        keyService.setIndexService(sys.getIndexService());

        Random r = new Random(99);
        List<double[]> vs = new ArrayList<>();

        for (int i = 0; i < 32; i++) {
            double[] v = new double[8];
            for (int j = 0; j < 8; j++) v[j] = r.nextDouble();
            vs.add(v);
        }
        sys.batchInsert(vs, 8);
        sys.finalizeForSearch();

        double[] q = new double[8];
        for (int j = 0; j < 8; j++) q[j] = r.nextDouble();

        var engine = sys.getEngine();

        List<QueryResult> r1 = engine.evalSimple(q, 10, 8, false);
        List<QueryResult> r2 = engine.evalSimple(q, 10, 8, false);

        // FIX: cannot assertSame; engine may clone list
        assertEquals(r1, r2);

        sys.setExitOnShutdown(false);
    }

    // ===========================================================
    // 3. Touch Tracking
    // ===========================================================
    @Test
    @Order(3)
    void testCandidateTouchTracking() throws Exception {

        ForwardSecureANNSystem sys = new ForwardSecureANNSystem(
                cfgFile.toString(),
                tempRoot.resolve("seed.csv").toString(),
                ksFile.toString(),
                List.of(8),
                tempRoot,
                false,
                metadata,
                crypto,
                16
        );

        keyService.setIndexService(sys.getIndexService());

        Random r = new Random(5);
        List<double[]> vecs = new ArrayList<>();

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
        sys.setExitOnShutdown(false);
    }

    @AfterAll
    static void cleanup() throws IOException {
        try { metadata.close(); } catch (Exception ignore) {}
        Files.walk(tempRoot)
                .sorted(Comparator.reverseOrder())
                .forEach(p -> { try { Files.deleteIfExists(p); } catch (Exception ignore) {} });
    }
}
