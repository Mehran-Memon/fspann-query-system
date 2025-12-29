package com.fspann.it;

import com.fspann.api.ForwardSecureANNSystem;
import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.index.paper.GFunctionRegistry;
import com.fspann.key.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;

import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MultiTableSystemIntegrationTest {

    Path temp;
    ForwardSecureANNSystem system;
    RocksDBMetadataManager metadata;

    @BeforeEach
    void setup() throws Exception {

        temp = Files.createTempDirectory("full-system");

        // 1. Declare dimensions (MANDATORY)
        List<Integer> dimensions = List.of(6);

        Path meta = temp.resolve("meta");
        Path pts  = temp.resolve("pts");
        Files.createDirectories(meta);
        Files.createDirectories(pts);

        metadata = RocksDBMetadataManager.create(meta.toString(), pts.toString());

        Path cfgPath = temp.resolve("cfg.json");
        Files.writeString(cfgPath, """
    {
      "paper": {
        "enabled": true,
        "tables": 3,
        "divisions": 4,
        "m": 6,
        "lambda": 3,
        "seed": 42
      }
    }
    """);

        SystemConfig cfg = SystemConfig.load(cfgPath.toString(), true);

        // 2. Initialize registry BEFORE system construction
        GFunctionRegistry.reset();
        GFunctionRegistry.initialize(
                List.of(
                        new double[]{1,2,3,4,5,6},
                        new double[]{2,3,4,5,6,7}
                ),
                6,                              // dimension
                cfg.getPaper().getM(),
                cfg.getPaper().getLambda(),
                cfg.getPaper().getSeed(),
                cfg.getPaper().getTables(),
                cfg.getPaper().getDivisions()
        );

        // 3. Key + crypto
        KeyManager km = new KeyManager(temp.resolve("keys.blob").toString());
        KeyRotationServiceImpl ks = new KeyRotationServiceImpl(
                km,
                new KeyRotationPolicy(Integer.MAX_VALUE, Long.MAX_VALUE),
                meta.toString(),
                metadata,
                null
        );

        AesGcmCryptoService crypto = new AesGcmCryptoService(
                new SimpleMeterRegistry(),
                ks,
                metadata
        );
        ks.setCryptoService(crypto);

        // 4. Construct system WITH dimensions
        system = new ForwardSecureANNSystem(
                cfgPath.toString(),
                temp.resolve("data.fvecs").toString(),
                temp.resolve("keys.blob").toString(),
                dimensions,
                temp,
                false,
                metadata,
                crypto,
                50
        );

        // 5. Insert vectors
        for (int i = 0; i < 20; i++) {
            system.insert(
                    "v" + i,
                    new double[]{i, i+1, i+2, i+3, i+4, i+5},
                    6
            );
        }

        // 6. Finalize
        system.flushAll();
        system.finalizeForSearch();
    }


    @Test
    void fullQueryWorks() {
        QueryToken t = system.createToken(
                new double[]{5,5,5,5,5,5}, 5, 6
        );
        List<QueryResult> res = system.getQueryServiceImpl().search(t);
        assertNotNull(res);
        assertTrue(res.size() <= t.getTopK());

        assertTrue(system.getQueryServiceImpl().getLastCandTotal() >= 0);
        assertTrue(system.getQueryServiceImpl().getLastCandDecrypted()
                >= system.getQueryServiceImpl().getLastReturned());
    }

    @AfterAll
    void shutdownOnce() throws Exception {
        system.shutdown();
        metadata.close();
    }
}
