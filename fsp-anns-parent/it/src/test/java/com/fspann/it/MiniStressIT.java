package com.fspann.it;

import com.fspann.api.ForwardSecureANNSystem;
import com.fspann.common.*;
import com.fspann.crypto.*;
import com.fspann.key.*;

import org.junit.jupiter.api.*;

import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class MiniStressIT {

    @Test
    void test1000Vectors() throws Exception {

        Path root = Files.createTempDirectory("stress_");
        Path meta = root.resolve("metadata");
        Path pts  = root.resolve("points");
        Path ks   = root.resolve("ks.bin");

        Files.createDirectories(meta);
        Files.createDirectories(pts);

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

        ForwardSecureANNSystem sys = new ForwardSecureANNSystem(
                "CFG","DATA",ks.toString(),
                List.of(16), root,false,m,crypto,128
        );

        Random r = new Random(1);
        for (int i = 0; i < 1000; i++) {
            double[] v = new double[16];
            for (int j = 0; j < 16; j++) v[j] = r.nextDouble();
            sys.insert("id" + i, v, 16);
        }

        sys.finalizeForSearch();

        double[] q = new double[16];
        for (int j = 0; j < 16; j++) q[j] = r.nextDouble();

        var out = sys.getEngine().evalSimple(q, 20, 16, false);
        assertFalse(out.isEmpty());
        sys.shutdown();
    }
}
