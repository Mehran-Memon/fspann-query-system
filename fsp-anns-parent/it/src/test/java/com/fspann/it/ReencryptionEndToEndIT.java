package com.fspann.it;

import com.fspann.api.ForwardSecureANNSystem;
import com.fspann.api.ApiSystemConfig;
import com.fspann.config.SystemConfig;
import com.fspann.common.*;
import com.fspann.crypto.*;
import com.fspann.key.*;

import org.junit.jupiter.api.*;

import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class ReencryptionEndToEndIT extends BaseSystemIT {

    @Test
    void testEndOfRunReencryption() throws Exception {

        Path root = Files.createTempDirectory("reenc_");
        Path meta = root.resolve("metadata");
        Path pts  = root.resolve("points");
        Path ks   = root.resolve("ks.bin");

        Files.createDirectories(meta);
        Files.createDirectories(pts);

        Path seed = root.resolve("seed.csv");
        Files.writeString(seed, "");

        RocksDBMetadataManager metadata =
                RocksDBMetadataManager.create(meta.toString(), pts.toString());

        KeyManager km = new KeyManager(ks.toString());
        KeyRotationPolicy pol = new KeyRotationPolicy(1000000, 99999999);
        KeyRotationServiceImpl ksrv =
                new KeyRotationServiceImpl(km, pol, meta.toString(), metadata, null);

        AesGcmCryptoService crypto = new AesGcmCryptoService(null, ksrv, metadata);
        ksrv.setCryptoService(crypto);

        Path cfgFile = Files.createTempFile(root, "cfg_", ".json");
        Files.writeString(cfgFile, """
        {
          "paper":{"enabled":true,"m":3,"divisions":3,"lambda":3,"seed":13},
          "reencryption":{"enabled":true},
          "lsh":{"numTables":0,"rowsPerBand":0,"probeShards":0},
          "output":{"exportArtifacts":false}
        }
        """);

        SystemConfig cfg = new ApiSystemConfig(cfgFile.toString()).getConfig();

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

        sys.insert("a", new double[]{1,2,3,4}, 4);
        sys.insert("b", new double[]{4,3,2,1}, 4);

        sys.finalizeForSearch();

        double[] q = new double[]{0.5,0.5,0.5,0.5};
        sys.getEngine().evalSimple(q, 10, 4, false);

        sys.setExitOnShutdown(false);

        assertTrue(ksrv.getCurrentVersion().getVersion() >= 1);
    }
}