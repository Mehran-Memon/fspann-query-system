package com.fspann.query;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.CryptoService;
import com.fspann.index.paper.GFunctionRegistry;
import com.fspann.index.paper.PartitionedIndexService;
import com.fspann.query.core.QueryTokenFactory;
import org.junit.jupiter.api.*;

import javax.crypto.SecretKey;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class QueryTokenFactoryTest {

    private QueryTokenFactory tokenFactory;
    private CryptoService crypto;
    private KeyLifeCycleService keyService;
    private PartitionedIndexService partition;
    private SystemConfig config;

    @BeforeEach
    void setUp() {

        List<double[]> sample = List.of(
                new double[]{1.0, 2.0, 3.0},
                new double[]{2.0, 1.0, 0.5},
                new double[]{1.5, 1.5, 1.5}
        );

        // ---------------- Registry (MANDATORY) ----------------
        GFunctionRegistry.reset();

        GFunctionRegistry.initialize(
                sample,     // List<double[]>
                3,          // dimension
                5,          // m
                10,         // lambda
                42L,        // baseSeed
                3,          // tables
                8           // divisions
        );

        crypto = mock(CryptoService.class);
        keyService = mock(KeyLifeCycleService.class);
        partition = mock(PartitionedIndexService.class);
        config = mock(SystemConfig.class);

        // Real PaperConfig (fields accessed directly)
        SystemConfig.PaperConfig pc = new SystemConfig.PaperConfig() {
            @Override public int getTables() { return 3; }
        };
        pc.divisions = 8;
        pc.m = 5;
        pc.lambda = 10;
        pc.seed = 42L;

        when(config.getPaper()).thenReturn(pc);

        // Crypto + key mocks
        SecretKey mockKey = mock(SecretKey.class);
        when(keyService.getCurrentVersion()).thenReturn(new KeyVersion(1, mockKey));
        when(crypto.encryptQuery(any(), any(), any())).thenReturn(new byte[]{1,2,3});

        tokenFactory =
                new QueryTokenFactory(crypto, keyService, partition, config, 8, 4);
    }

    @Test
    void testCreateQueryToken_OK() {
        double[] q = new double[]{1.0, 2.0, 3.0};

        QueryToken tok = tokenFactory.create(q, 10);

        assertNotNull(tok);
        assertEquals(10, tok.getTopK());
        assertEquals(3, tok.getNumTables());
        assertEquals(3, tok.getCodesByTable().length);
        assertNotNull(tok.getEncryptedQuery());
        assertNotNull(tok.getIv());
    }

    @Test
    void testCreateFailsWhenRegistryMissing() {
        GFunctionRegistry.reset();

        assertThrows(
                IllegalStateException.class,
                () -> tokenFactory.create(new double[]{1,2,3}, 5)
        );
    }

    @Test
    void testInvalidTopK() {
        assertThrows(
                IllegalArgumentException.class,
                () -> tokenFactory.create(new double[]{1,2}, 0)
        );
    }
}
