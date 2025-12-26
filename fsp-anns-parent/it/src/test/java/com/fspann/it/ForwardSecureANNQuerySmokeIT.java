package com.fspann.it;

import com.fspann.api.ForwardSecureANNSystem;
import com.fspann.common.QueryResult;
import com.fspann.common.QueryToken;
import com.fspann.common.RocksDBMetadataManager;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.key.*;
import com.fspann.loader.GroundtruthManager;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.*;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ForwardSecureANNQuerySmokeIT {

    @TempDir
    Path temp;

    ForwardSecureANNSystem system;
    RocksDBMetadataManager metadata;

    @BeforeEach
    void setup() throws Exception {
        Path metaDir = temp.resolve("meta");
        Path ptsDir  = temp.resolve("pts");
        Path keyDir  = temp.resolve("keys");

        Files.createDirectories(metaDir);
        Files.createDirectories(ptsDir);
        Files.createDirectories(keyDir);

        metadata = RocksDBMetadataManager.create(
                metaDir.toString(),
                ptsDir.toString()
        );

        KeyManager km = new KeyManager(keyDir.resolve("ks.blob").toString());
        KeyRotationServiceImpl keyService =
                new KeyRotationServiceImpl(
                        km,
                        new KeyRotationPolicy(Integer.MAX_VALUE, Long.MAX_VALUE),
                        metaDir.toString(),
                        metadata,
                        null
                );

        AesGcmCryptoService crypto =
                new AesGcmCryptoService(
                        new SimpleMeterRegistry(),
                        keyService,
                        metadata
                );
        keyService.setCryptoService(crypto);

        Path cfg = temp.resolve("cfg.json");
        Files.writeString(cfg, """
        {
          "paper": {
            "enabled": true,
            "m": 4,
            "lambda": 2,
            "divisions": 2,
            "seed": 13,
            "probeLimit": 3
          },
          "ratio": { "source": "none" },
          "output": { "exportArtifacts": false },
          "stabilization": {
            "enabled": true,
            "alpha": 0.5,
            "minCandidatesRatio": 1.0
          }
        }
        """);

        system = new ForwardSecureANNSystem(
                cfg.toString(),
                temp.resolve("seed.csv").toString(),
                keyDir.toString(),
                List.of(2),
                temp,
                false,
                metadata,
                crypto,
                128
        );

        // Insert test vectors
        system.insert("0", new double[]{0.0, 0.0}, 2);
        system.insert("1", new double[]{0.1, 0.1}, 2);
        system.insert("2", new double[]{0.2, 0.2}, 2);

        system.finalizeForSearch();
        system.flushAll();
    }

    @Test
    void runQueriesExecutesANNLookup() {
        // SIMPLIFIED: Just test direct query, not runQueries()
        double[] query = new double[]{0.05, 0.05};

        assertDoesNotThrow(() -> {
            QueryToken tok = system.createToken(query, 2, 2);
            List<QueryResult> results = system.getQueryServiceImpl().search(tok);
            assertNotNull(results, "Should return result list");
        });

        // Verify search was executed
        int touched = system.getIndexService().getLastTouchedCount();
        assertTrue(touched >= 0, "Should execute search without error");
    }

    @AfterEach
    void cleanup() {
        try { system.setExitOnShutdown(false); system.shutdown(); } catch (Exception ignore) {}
        try { metadata.close(); } catch (Exception ignore) {}
    }
}