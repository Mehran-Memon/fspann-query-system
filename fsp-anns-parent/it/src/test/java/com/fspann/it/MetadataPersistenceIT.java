package com.fspann.it;

import com.fspann.api.ForwardSecureANNSystem;
import com.fspann.crypto.*;
import com.fspann.key.*;
import com.fspann.common.*;

import org.junit.jupiter.api.*;

import java.nio.file.*;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class MetadataPersistenceIT {

    @Test
    void testRestoreIndex() throws Exception {

        Path root = Files.createTempDirectory("meta_restore_");
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
                List.of(4), root,false,m,crypto,32
        );

        sys.insert("z", new double[]{9,9,9,9},4);
        sys.flushAll();

        int restored = sys.restoreIndexFromDisk(ksrv.getCurrentVersion().getVersion());
        assertEquals(1, restored);
    }
}
