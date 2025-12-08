package com.fspann.it;

import com.fspann.api.ForwardSecureANNSystem;

import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.key.*;
import com.fspann.common.*;

import org.junit.jupiter.api.*;

import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class MiniStressIT extends BaseSystemIT {

    @Test
    void test1000VectorsStress() throws Exception {

        Path root = Files.createTempDirectory("stress_");
        Path meta = root.resolve("metadata");
        Path pts  = root.resolve("points");
        Path ks   = root.resolve("ks.bin");
        Path seed = root.resolve("seed.csv");

        Files.createDirectories(meta);
        Files.createDirectories(pts);
        Files.writeString(seed, "");

        RocksDBMetadataManager m =
                RocksDBMetadataManager.create(meta.toString(), pts.toString());

        KeyRotationPolicy pol = new KeyRotationPolicy(999999, 999999);
        KeyRotationServiceImpl ksrv =
                new KeyRotationServiceImpl(new KeyManager(ks.toString()),
                        pol, meta.toString(), m, null);

        AesGcmCryptoService crypto = new AesGcmCryptoService(null, ksrv, m);
        ksrv.setCryptoService(crypto);

        ForwardSecureANNSystem sys = new ForwardSecureANNSystem(
                seed.toString(), seed.toString(), ks.toString(),
                List.of(16), root, false, m, crypto, 128
        );
        ksrv.setIndexService(sys.getIndexService());

        Random r = new Random(1);
        for (int i = 0; i < 1000; i++) {
            double[] v = new double[16];
            for (int j = 0; j < 16; j++)
                v[j] = r.nextDouble();
            sys.insert("id" + i, v, 16);
        }

        sys.finalizeForSearch();

        double[] q = new double[16];
        for (int j = 0; j < 16; j++)
            q[j] = r.nextDouble();

        var out = sys.getEngine().evalSimple(q, 20, 16, false);

        assertFalse(out.isEmpty());

        sys.shutdown();
    }
}
