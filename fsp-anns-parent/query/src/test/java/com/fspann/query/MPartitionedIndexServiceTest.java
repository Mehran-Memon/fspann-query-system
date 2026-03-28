package com.fspann.query;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.index.paper.GFunctionRegistry;
import com.fspann.index.paper.PartitionedIndexService;
import com.fspann.key.*;
import org.junit.jupiter.api.*;

import javax.crypto.SecretKey;
import java.nio.file.Files;
import java.nio.file.Path;
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
    void setUp() throws Exception {

        // ---------------- Registry ----------------
        GFunctionRegistry.reset();
        GFunctionRegistry.initialize(
                List.of(
                        new double[]{1, 2, 3},
                        new double[]{2, 1, 0.5},
                        new double[]{1.5, 1.5, 1.5}
                ),
                3, 5, 10, 42L, 3, 8
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
          },
          "runtime": {
            "maxCandidateFactor": 5,
            "earlyStopCandidates": 1000,
            "maxRelaxationDepth": 3
          }
        }
        """);

        SystemConfig config = SystemConfig.load(cfg.toString(), true);

        // ---------------- Metadata ----------------
        meta = mock(RocksDBMetadataManager.class);
        StorageMetrics sm = mock(StorageMetrics.class);
        when(meta.getStorageMetrics()).thenReturn(sm);
        when(sm.getSnapshot()).thenReturn(
                new StorageMetrics.StorageSnapshot(
                        0, 0, 0,
                        new ConcurrentHashMap<>(),
                        new ConcurrentHashMap<>()
                )
        );

        // ---------------- Crypto ----------------
        crypto = mock(AesGcmCryptoService.class);
        keyService = mock(KeyRotationServiceImpl.class);

        SecretKey key = mock(SecretKey.class);
        when(keyService.getCurrentVersion())
                .thenReturn(new KeyVersion(1, key));

        index =
                new PartitionedIndexService(
                        meta,
                        config,
                        keyService,
                        crypto
                );
    }

    @Test
    void testInsertEncryptsAndStores() throws Exception {

        double[] v = new double[]{1, 2, 3};

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
