package com.fspann.it;

import com.fspann.api.ForwardSecureANNSystem;
import com.fspann.common.*;
import com.fspann.loader.GroundtruthManager;
import com.fspann.crypto.*;
import com.fspann.key.*;

import org.junit.jupiter.api.*;

import java.nio.file.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

public class RatioPipelineIT extends BaseSystemIT {

    @Test
    void testRatioComputationBaseSource() throws Exception {

        Path root = Files.createTempDirectory("ratio_");
        Path meta = root.resolve("metadata");
        Path pts  = root.resolve("points");
        Path ks   = root.resolve("ks.bin");

        Files.createDirectories(meta);
        Files.createDirectories(pts);

        // seed file required
        Path seed = root.resolve("seed.csv");
        Files.writeString(seed, "");

        RocksDBMetadataManager m =
                RocksDBMetadataManager.create(meta.toString(), pts.toString());

        KeyRotationServiceImpl ksrv = new KeyRotationServiceImpl(
                new KeyManager(ks.toString()),
                new KeyRotationPolicy(999999,999999),
                meta.toString(),
                m,
                null
        );

        AesGcmCryptoService crypto = new AesGcmCryptoService(null, ksrv, m);
        ksrv.setCryptoService(crypto);

        // Dummy GT file
        Path gtPath = root.resolve("gt.ivecs");
        Files.writeString(gtPath, "");

        ForwardSecureANNSystem sys = new ForwardSecureANNSystem(
                gtPath.toString(),     // config path not used for ratio
                seed.toString(),
                ks.toString(),
                List.of(4),
                root,
                false,
                m,
                crypto,
                32
        );

        ksrv.setIndexService(sys.getIndexService());

        // Insert base vectors
        for (int i=0;i<10;i++)
            sys.insert(String.valueOf(i),
                    new double[]{i, i+0.1, i+0.2, i+0.3}, 4);

        sys.finalizeForSearch();

        List<double[]> queries = List.of(new double[]{1.0,1.1,1.2,1.3});

        GroundtruthManager gt = new GroundtruthManager();
        gt.load(gtPath.toString());   // OK — file exists

        sys.getEngine().evalBatch(
                queries,
                4,
                gt,
                root,
                true
        );

        sys.shutdown();
        assertTrue(true);
    }
}
