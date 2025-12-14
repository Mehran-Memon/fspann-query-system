package com.fspann.it;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.index.paper.PartitionedIndexService;
import com.fspann.key.*;
import com.fspann.query.core.QueryTokenFactory;
import com.fspann.query.service.QueryServiceImpl;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.*;

class PartitionedIndexQueryIT {
    private static final Logger logger =
            LoggerFactory.getLogger(PartitionedIndexQueryIT.class);

    static PartitionedIndexService index;
    static QueryServiceImpl queryService;
    static QueryTokenFactory tokenFactory;
    static KeyRotationServiceImpl keyService;
    static RocksDBMetadataManager metadata;

    @BeforeAll
    static void setup() throws Exception {
        Path root = Files.createTempDirectory("fspann-it");
        Path meta = root.resolve("meta");
        Path pts  = root.resolve("pts");

        Files.createDirectories(meta);
        Files.createDirectories(pts);

        metadata = RocksDBMetadataManager.create(meta.toString(), pts.toString());

        Path cfgFile = root.resolve("cfg.json");
        Files.writeString(cfgFile, """
        {
          "paper": { "enabled": true, "divisions": 4, "m": 12, "lambda": 3, "seed": 42 },
          "partitionedIndexingEnabled": true,
          "reencryptionEnabled": true,
          "lsh": { "numTables": 0 }
        }
        """);

        SystemConfig cfg = SystemConfig.load(cfgFile.toString(), true);

        KeyManager km = new KeyManager(root.resolve("keys.blob").toString());
        keyService = new KeyRotationServiceImpl(
                km,
                new KeyRotationPolicy(Integer.MAX_VALUE, Long.MAX_VALUE),
                root.resolve("rot").toString(),
                metadata,
                null
        );

        AesGcmCryptoService crypto =
                new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadata);
        keyService.setCryptoService(crypto);

        index =
                new PartitionedIndexService(
                        metadata,
                        cfg,
                        keyService,
                        crypto
                );

        tokenFactory = new QueryTokenFactory(
                crypto, keyService, index, cfg, cfg.getPaper().getDivisions()
        );

        queryService = new QueryServiceImpl(index, crypto, keyService, tokenFactory, cfg);
    }

    @Test
    void testBasicInsertAndQuery() {
        for (int i = 0; i < 1000; i++) {
            // Insert more dense vectors around the query vector
            double offset = (i % 100) * 0.1;
            index.insert("v" + i, new double[]{
                    10.0 + offset,      // Centered near query [10, 11, 12]
                    11.0 + offset,
                    12.0 + offset
            });
        }

        index.finalizeForSearch();

        // ===== CRITICAL: Force flush to metadata =====
        try {
            metadata.flush();
            if (logger.isDebugEnabled()) {
                logger.debug("Metadata flushed after finalization");
            }
        } catch (Exception e) {
            logger.warn("Flush failed", e);
        }

        QueryToken token = tokenFactory.create(new double[]{10, 11, 12}, 10);
        List<QueryResult> results = queryService.search(token);

        // Results should not be empty now (within ~0.1 of query)
        assertFalse(results.isEmpty(), "Query should find candidates near [10,11,12]");
        assertTrue(results.size() <= 10);
        assertTrue(queryService.getLastCandDecrypted() <= queryService.getLastCandTotal());
    }
}
