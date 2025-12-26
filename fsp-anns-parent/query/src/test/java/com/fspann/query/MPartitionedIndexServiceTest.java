package com.fspann.query;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.index.paper.GFunctionRegistry;
import com.fspann.index.paper.PartitionedIndexService;
import com.fspann.key.*;
import org.junit.jupiter.api.*;

import javax.crypto.SecretKey;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class MPartitionedIndexServiceTest {

    private PartitionedIndexService index;
    private RocksDBMetadataManager meta;
    private AesGcmCryptoService crypto;
    private KeyRotationServiceImpl keyService;

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

        meta = mock(RocksDBMetadataManager.class);
        crypto = mock(AesGcmCryptoService.class);
        keyService = mock(KeyRotationServiceImpl.class);

        // Storage metrics mock
        StorageMetrics sm = mock(StorageMetrics.class);
        when(meta.getStorageMetrics()).thenReturn(sm);
        when(sm.getSnapshot()).thenReturn(
                new StorageMetrics.StorageSnapshot(
                        0L,0L,0L,
                        new ConcurrentHashMap<>(),
                        new ConcurrentHashMap<>()
                )
        );

        // Key lifecycle
        SecretKey key = mock(SecretKey.class);
        when(keyService.getCurrentVersion()).thenReturn(new KeyVersion(1, key));

        // Config (REAL PaperConfig)
        SystemConfig cfg = mock(SystemConfig.class);
        SystemConfig.PaperConfig pc = new SystemConfig.PaperConfig() {
            @Override public int getTables() { return 3; }
        };
        pc.divisions = 8;
        pc.m = 5;
        pc.lambda = 10;
        pc.seed = 42L;

        when(cfg.getPaper()).thenReturn(pc);

        SystemConfig.RuntimeConfig rc = mock(SystemConfig.RuntimeConfig.class);
        when(rc.getMaxCandidateFactor()).thenReturn(5);
        when(rc.getEarlyStopCandidates()).thenReturn(1000);
        when(rc.getMaxRelaxationDepth()).thenReturn(3);
        when(cfg.getRuntime()).thenReturn(rc);

        index =
                new PartitionedIndexService(meta, cfg, keyService, crypto);
    }

    @Test
    void testInsertEncryptsAndStores() throws Exception {
        double[] v = new double[]{1.0, 2.0, 3.0};

        EncryptedPoint ep = mock(EncryptedPoint.class);
        when(ep.getId()).thenReturn("vec-1");
        when(ep.getVectorLength()).thenReturn(3);

        when(crypto.encrypt(
                eq("vec-1"),
                eq(v),
                any(KeyVersion.class)
        )).thenReturn(ep);

        doNothing().when(meta).saveEncryptedPoint(ep);

        index.insert("vec-1", v);

        verify(crypto).encrypt(eq("vec-1"), eq(v), any());
        verify(meta).saveEncryptedPoint(ep);
    }

}
