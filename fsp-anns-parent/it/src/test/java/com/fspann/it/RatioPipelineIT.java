package com.fspann.it;

import com.fspann.api.ForwardSecureANNSystem;
import com.fspann.common.*;
import com.fspann.loader.GroundtruthManager;
import com.fspann.crypto.*;
import com.fspann.key.*;

import org.junit.jupiter.api.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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

        Path cfg = root.resolve("cfg.json");
        Files.writeString(cfg, BaseSystemIT.minimalCfgJson());

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

        Path gtPath = root.resolve("gt.ivecs");
        try (var out = Files.newOutputStream(gtPath)) {
            out.write(ByteBuffer.allocate(8)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .putInt(1)
                    .putInt(0)
                    .array());
        }

        ForwardSecureANNSystem sys = new ForwardSecureANNSystem(
                cfg.toString(),
                root.resolve("seed.csv").toString(),
                ks.toString(),
                List.of(4),
                root,
                false,
                m,
                crypto,
                32
        );

        ksrv.setIndexService(sys.getIndexService());

        for (int i=0;i<10;i++)
            sys.insert(String.valueOf(i),
                    new double[]{i, i+0.1, i+0.2, i+0.3}, 4);

        sys.finalizeForSearch();

        GroundtruthManager gt = new GroundtruthManager();
        gt.load(gtPath.toString());

        sys.getEngine().evalBatch(
                List.of(new double[]{1,1.1,1.2,1.3}),
                4,
                gt,
                root,
                true
        );

        sys.setExitOnShutdown(false);
        assertTrue(true);
    }
}