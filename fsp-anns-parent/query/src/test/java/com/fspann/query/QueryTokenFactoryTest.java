package com.fspann.query;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.CryptoService;
import com.fspann.index.paper.GFunctionRegistry;
import com.fspann.index.paper.PartitionedIndexService;
import com.fspann.query.core.QueryTokenFactory;
import org.junit.jupiter.api.*;

import javax.crypto.SecretKey;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class QueryTokenFactoryTest {

    private QueryTokenFactory tokenFactory;
    private CryptoService crypto;
    private KeyLifeCycleService keyService;
    private PartitionedIndexService index;
    private SystemConfig config;

    @BeforeEach
    void setUp() throws Exception {

        // ---------------- Registry ----------------
        GFunctionRegistry.reset();
        GFunctionRegistry.initialize(
                List.of(
                        new double[]{1, 2, 3},
                        new double[]{2, 1, 0.5},
                        new double[]{1.5, 1.5, 1.5}
                ),
                3,      // dim
                5,      // m
                10,     // lambda
                42L,
                3,      // tables
                8       // divisions
        );

        // ---------------- Config ----------------
        Path cfg = Files.createTempFile("cfg", ".json");
        Files.writeString(cfg, """
        {
          "paper": {
            "enabled": true,
            "m": 5,
            "lambda": 10,
            "divisions": 8,
            "tables": 3,
            "seed": 42
          }
        }
        """);

        config = SystemConfig.load(cfg.toString(), true);

        // ---------------- Crypto + Keys ----------------
        crypto = mock(CryptoService.class);
        keyService = mock(KeyLifeCycleService.class);
        index = mock(PartitionedIndexService.class);

        SecretKey key = mock(SecretKey.class);
        when(keyService.getCurrentVersion())
                .thenReturn(new KeyVersion(1, key));

        when(crypto.encryptQuery(any(), any(), any()))
                .thenReturn(new byte[]{1, 2, 3});

        tokenFactory =
                new QueryTokenFactory(
                        crypto,
                        keyService,
                        index,
                        config,
                        config.getPaper().getDivisions(),
                        config.getPaper().getTables()
                );
    }

    @Test
    void testCreateQueryToken_OK() {
        double[] q = new double[]{1.0, 2.0, 3.0};

        QueryToken tok = tokenFactory.create(q, 10);

        assertNotNull(tok);
        assertEquals(10, tok.getTopK());

        // Tables
        assertEquals(3, tok.getNumTables());

        // Hashes (MSANNP v2)
        int[][] hashes = tok.getHashesByTable();
        assertNotNull(hashes);
        assertEquals(3, hashes.length);

        for (int[] h : hashes) {
            assertNotNull(h);
            assertEquals(8, h.length); // divisions
        }

        // Legacy buckets are intentionally EMPTY
        assertNotNull(tok.getTableBuckets());
        assertTrue(tok.getTableBuckets().isEmpty(),
                "Legacy multiprobe buckets must be empty in MSANNP v2");

        // Encryption material
        assertNotNull(tok.getEncryptedQuery());
        assertNotNull(tok.getIv());
    }

    @Test
    void testFailsWhenRegistryMissing() {
        GFunctionRegistry.reset();
        assertThrows(
                IllegalStateException.class,
                () -> tokenFactory.create(new double[]{1, 2, 3}, 5)
        );
    }

    @Test
    void testInvalidTopK() {
        assertThrows(
                IllegalArgumentException.class,
                () -> tokenFactory.create(new double[]{1, 2, 3}, 0)
        );
    }
}
