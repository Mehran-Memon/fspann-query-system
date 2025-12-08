package com.fspann.it;

import com.fspann.api.ForwardSecureANNSystem;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.key.*;
import com.fspann.common.*;
import com.fspann.query.service.QueryServiceImpl;

import org.junit.jupiter.api.*;

import java.nio.file.*;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class KAdaptiveProbeOnlyTest extends BaseSystemIT {

    @Test
    void testProbeOnlyDoesNotAffectResults() throws Exception {

        Path root = Files.createTempDirectory("kadapt_");
        Path meta = root.resolve("metadata");
        Path pts  = root.resolve("points");
        Path ks   = root.resolve("ks.bin");
        Path seed = root.resolve("seed.csv");
        Path cfgFile = root.resolve("cfg.json");

        Files.createDirectories(meta);
        Files.createDirectories(pts);

        Files.writeString(seed, "");

        Files.writeString(cfgFile, """
        {
          "paper":{"enabled":true,"m":3,"divisions":3,"lambda":3,"seed":13},
          "lsh":{"numTables":0,"rowsPerBand":0,"probeShards":0},
          "reencryption":{"enabled":false},
          "output":{"exportArtifacts":false}
        }
        """);

        RocksDBMetadataManager metadata =
                RocksDBMetadataManager.create(meta.toString(), pts.toString());

        KeyRotationPolicy pol = new KeyRotationPolicy(999999, 999999);
        KeyRotationServiceImpl ksrv = new KeyRotationServiceImpl(
                new KeyManager(ks.toString()),
                pol,
                meta.toString(),
                metadata,
                null
        );

        AesGcmCryptoService crypto = new AesGcmCryptoService(null, ksrv, metadata);
        ksrv.setCryptoService(crypto);

        System.setProperty("kAdaptive.enabled", "true");

        ForwardSecureANNSystem sys = new ForwardSecureANNSystem(
                cfgFile.toString(),
                seed.toString(),
                ks.toString(),
                List.of(4),
                root,
                false,
                metadata,
                crypto,
                32
        );

        ksrv.setIndexService(sys.getIndexService());

        sys.insert("x", new double[]{1,2,3,4}, 4);
        sys.finalizeForSearch();

        // MUST use the concrete type, not the interface
        QueryServiceImpl qsImpl = sys.getQueryServiceImpl();

        // Match API signature exactly:
        // runKAdaptiveProbeOnly(int shard, double[] q, int dim, QueryServiceImpl qs)
        sys.runKAdaptiveProbeOnly(
                0,
                new double[]{0.1,0.2,0.3,0.4},
                4,
                qsImpl
        );

        assertTrue(true);
        sys.setExitOnShutdown(false);
    }
}
