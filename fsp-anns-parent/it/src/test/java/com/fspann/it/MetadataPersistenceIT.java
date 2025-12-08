package com.fspann.it;

import com.fspann.api.ForwardSecureANNSystem;
import com.fspann.crypto.*;
import com.fspann.key.*;
import com.fspann.common.*;

import org.junit.jupiter.api.*;

import java.nio.file.*;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class MetadataPersistenceIT extends BaseSystemIT{

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

        Path cfgFile = Files.createTempFile(root, "cfg_", ".json");
        Files.writeString(cfgFile, """
{
  "paper": { "enabled": false },
  "lsh":   { "numTables": 0, "rowsPerBand": 0, "probeShards": 0 },
  "eval":  { "computePrecision": false },
  "ratio": { "source": "base" },
  "opsThreshold": 999999,
  "ageThresholdMs": 999999,
  "numShards": 4,
  "output": { "exportArtifacts": false, "resultsDir": "out" },
  "reencryption": { "enabled": false }
}
""");


        ForwardSecureANNSystem sys = new ForwardSecureANNSystem(
                cfgFile.toString(),
                "DATA",
                ks.toString(),
                List.of(4),
                root,
                false,
                m,
                crypto,
                32
        );

        // use batchInsert instead of insert()
        sys.batchInsert(List.of(new double[]{9,9,9,9}), 4);
        sys.flushAll();

        int restored = sys.restoreIndexFromDisk(ksrv.getCurrentVersion().getVersion());
        assertEquals(1, restored);
    }
}
