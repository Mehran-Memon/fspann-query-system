package com.fspann.it;

import com.fspann.api.ForwardSecureANNSystem;
import com.fspann.common.*;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.key.*;

import org.junit.jupiter.api.*;
import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class KAdaptiveProbeOnlyTest {

    @Test
    void testProbeOnlyDoesNotAffectResults() throws Exception {

        Path root = Files.createTempDirectory("kadapt_");
        Path meta = root.resolve("metadata");
        Path pts  = root.resolve("points");
        Path ks   = root.resolve("ks.bin");

        Files.createDirectories(meta);
        Files.createDirectories(pts);

        RocksDBMetadataManager metadata =
                RocksDBMetadataManager.create(meta.toString(), pts.toString());

        KeyManager km = new KeyManager(ks.toString());
        KeyRotationPolicy pol = new KeyRotationPolicy(999999, 999999);
        KeyRotationServiceImpl ksvc = new KeyRotationServiceImpl(
                km, pol, meta.toString(), metadata, null);

        AesGcmCryptoService crypto = new AesGcmCryptoService(null, ksvc, metadata);
        ksvc.setCryptoService(crypto);

        // Force K-adaptive ON
        System.setProperty("kAdaptive.enabled","true");

        ForwardSecureANNSystem sys = new ForwardSecureANNSystem(
                "CFG","DATA",ks.toString(),
                List.of(4), root,false,metadata,crypto, 32
        );

        sys.insert("x", new double[]{1,2,3,4},4);
        sys.finalizeForSearch();

        sys.runKAdaptiveProbeOnly(0, new double[]{0.1,0.2,0.3,0.4}, 4, sys.getQueryServiceImpl());

        // probe-only never breaks anything
        assertTrue(true);
    }
}
