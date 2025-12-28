package com.fspann.it;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.index.paper.GFunctionRegistry;
import com.fspann.index.paper.PartitionedIndexService;
import com.fspann.key.*;
import com.fspann.query.core.QueryTokenFactory;
import com.fspann.query.service.QueryServiceImpl;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class MultiTableMSANNPIntegrationTest {

    Path tempDir;
    RocksDBMetadataManager metadata;
    PartitionedIndexService indexService;
    QueryTokenFactory tokenFactory;
    QueryServiceImpl queryService;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("msannp-test");
        Path meta = tempDir.resolve("meta");
        Path pts  = tempDir.resolve("pts");
        Files.createDirectories(meta);
        Files.createDirectories(pts);


        metadata = RocksDBMetadataManager.create(meta.toString(), pts.toString());

        Path cfgFile = tempDir.resolve("cfg.json");
        Files.writeString(cfgFile, """
        {
          "paper": { "enabled": true, "tables": 3, "divisions": 4, "m": 6, "seed": 123 }
        }
        """);

        SystemConfig cfg = SystemConfig.load(cfgFile.toString(), true);

        GFunctionRegistry.reset();
        GFunctionRegistry.initialize(
                List.of(
                        new double[]{1,2,3,4,5,6},
                        new double[]{2,3,4,5,6,7}
                ),
                6,
                cfg.getPaper().m,
                cfg.getPaper().lambda,
                cfg.getPaper().seed,
                cfg.getPaper().getTables(),
                cfg.getPaper().divisions
        );

        KeyManager km = new KeyManager(tempDir.resolve("keys.blob").toString());
        KeyRotationServiceImpl ks = new KeyRotationServiceImpl(
                km, new KeyRotationPolicy(Integer.MAX_VALUE, Long.MAX_VALUE),
                meta.toString(), metadata, null
        );

        AesGcmCryptoService crypto = new AesGcmCryptoService(
                new SimpleMeterRegistry(), ks, metadata
        );
        ks.setCryptoService(crypto);

        indexService = new PartitionedIndexService(metadata, cfg, ks, crypto);
        tokenFactory = new QueryTokenFactory(crypto, ks, indexService, cfg, 4, 4);
        queryService = new QueryServiceImpl(indexService, crypto, ks, tokenFactory, cfg);

        bootstrapIndex();
    }

    private void bootstrapIndex() throws Exception {
        indexService.insert("__boot__", new double[]{0,0,0,0,0,0});
        indexService.finalizeForSearch();
    }

    @Test
    void tokenHasMultipleTables() {
        QueryToken tok = tokenFactory.create(new double[]{1,2,3,4,5,6}, 5);
        assertEquals(3, tok.getCodesByTable().length);
    }

    @Test
    void multiTableQueryWorks() {
        QueryToken tok = tokenFactory.create(new double[]{1,2,3,4,5,6}, 5);
        List<QueryResult> res = queryService.search(tok);
        assertNotNull(res);
    }

    @AfterEach
    void cleanup() throws Exception {
        metadata.close();
        Files.walk(tempDir).sorted(Comparator.reverseOrder()).forEach(p -> {
            try { Files.delete(p); } catch (Exception ignored) {}
        });
    }
}
