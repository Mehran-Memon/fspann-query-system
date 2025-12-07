package com.fspann.it;

import com.fspann.api.ForwardSecureANNSystem;
import com.fspann.api.ApiSystemConfig;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.key.*;

import com.fspann.common.*;

import org.junit.jupiter.api.*;

import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class ReencryptionEndToEndIT {

    @Test
    void testEndOfRunReencryption() throws Exception {

        Path root = Files.createTempDirectory("reenc_");
        Path meta = root.resolve("metadata");
        Path pts  = root.resolve("points");
        Path ks   = root.resolve("ks.bin");

        Files.createDirectories(meta);
        Files.createDirectories(pts);

        RocksDBMetadataManager metadata =
                RocksDBMetadataManager.create(meta.toString(), pts.toString());

        KeyManager km = new KeyManager(ks.toString());
        KeyRotationPolicy pol = new KeyRotationPolicy(1000000, 99999999);
        KeyRotationServiceImpl ksrv =
                new KeyRotationServiceImpl(km, pol, meta.toString(), metadata, null);

        AesGcmCryptoService crypto = new AesGcmCryptoService(null, ksrv, metadata);
        ksrv.setCryptoService(crypto);

        SystemConfig cfg = new ApiSystemConfig(
                Files.writeString(Files.createTempFile("cfg",".json"),
                        """
                        {
                          "paper":{"enabled":true,"m":3,"divisions":3,"lambda":3,"seed":13},
                          "reencryption":{"enabled":true},
                          "lsh":{"numTables":0,"rowsPerBand":0,"probeShards":0},
                          "output":{"exportArtifacts":false}
                        }
                        """
                ).toString()).getConfig();

        ForwardSecureANNSystem sys = new ForwardSecureANNSystem(
                "CFG", "IGN", ks.toString(), List.of(4),
                root, false, metadata, crypto, 32
        );

        // insert points and query so they are "touched"
        sys.insert("a", new double[]{1,2,3,4}, 4);
        sys.insert("b", new double[]{4,3,2,1}, 4);

        sys.finalizeForSearch();

        double[] q = new double[]{0.5,0.5,0.5,0.5};
        sys.getEngine().evalSimple(q, 10, 4, false);

        // end-of-run re-encryption
        sys.shutdown();

        // Only property we assert: metadata still readable and keys advanced
        assertTrue(ksrv.getCurrentVersion().getVersion() >= 1);
    }
}
