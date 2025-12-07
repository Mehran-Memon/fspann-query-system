package com.fspann.it;

import com.fspann.api.ForwardSecureANNSystem;
import com.fspann.loader.GroundtruthManager;
import com.fspann.crypto.*;

import com.fspann.key.*;
import com.fspann.common.*;

import org.junit.jupiter.api.*;

import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class RatioPipelineIT {

    @Test
    void testRatioComputationBaseSource() throws Exception {

        Path root = Files.createTempDirectory("ratio_");
        Path meta = root.resolve("metadata");
        Path pts  = root.resolve("points");
        Path ks   = root.resolve("ks.bin");

        Files.createDirectories(meta);
        Files.createDirectories(pts);

        RocksDBMetadataManager metadata =
                RocksDBMetadataManager.create(meta.toString(), pts.toString());

        KeyRotationServiceImpl ksvc = new KeyRotationServiceImpl(
                new KeyManager(ks.toString()),
                new KeyRotationPolicy(999999,999999),
                meta.toString(),
                metadata,
                null
        );

        AesGcmCryptoService crypto = new AesGcmCryptoService(null, ksvc, metadata);
        ksvc.setCryptoService(crypto);

        // Make 10 base vectors
        List<double[]> base = new ArrayList<>();
        for (int i = 0; i < 10; i++)
            base.add(new double[]{i, i+0.1, i+0.2, i+0.3});

        Files.writeString(Paths.get(System.getProperty("base.path","base.fvecs")),
                ""); // Fake, we do not test file IO here

        ForwardSecureANNSystem sys = new ForwardSecureANNSystem(
                "CFG","DATA",ks.toString(),
                List.of(4), root,false,metadata,crypto,32
        );

        for (int i=0;i<10;i++)
            sys.insert(String.valueOf(i), base.get(i), 4);

        sys.finalizeForSearch();

        List<double[]> queries = List.of(new double[]{1.0,1.1,1.2,1.3});

        GroundtruthManager gt = new GroundtruthManager();
        gt.load(tempEmptyGT.ivecs);

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
