package com.fspann.it;

import com.fspann.common.*;
import com.fspann.config.SystemConfig;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.index.paper.PartitionedIndexService;
import com.fspann.key.*;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;
import java.util.Map;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class CRUDSemanticsIT {

    @Test
    void testDeleteVisibility() throws Exception {
        Path root = Files.createTempDirectory("fspann-crud");
        Path meta = root.resolve("meta");
        Path pts  = root.resolve("pts");

        Files.createDirectories(meta);
        Files.createDirectories(pts);

        RocksDBMetadataManager metadata =
                RocksDBMetadataManager.create(meta.toString(), pts.toString());

        Path cfgFile = root.resolve("cfg.json");
        Files.writeString(cfgFile, """
        {
          "paper": { "enabled": true, "divisions": 3, "m": 6, "seed": 1 },
          "partitionedIndexingEnabled": true
        }
        """);

        SystemConfig cfg = SystemConfig.load(cfgFile.toString(), true);

        KeyManager km = new KeyManager(root.resolve("keys.blob").toString());
        KeyRotationServiceImpl keyService =
                new KeyRotationServiceImpl(
                        km,
                        new KeyRotationPolicy(Integer.MAX_VALUE, Long.MAX_VALUE),
                        root.resolve("rot").toString(),
                        metadata,
                        null
                );

        AesGcmCryptoService crypto =
                new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadata);
        keyService.setCryptoService(crypto);

        PartitionedIndexService index =
                new PartitionedIndexService(
                        metadata,
                        cfg,
                        keyService,
                        crypto
                );

        index.insert("x", new double[]{1, 1, 1});
        index.insert("y", new double[]{2, 2, 2});
        index.finalizeForSearch();

        assertFalse(metadata.isDeleted("x"));

        metadata.updateVectorMetadata("x", Map.of("deleted", "true"));
        index.delete("x");

        assertTrue(metadata.isDeleted("x"));
        assertEquals(1, index.countActiveInPartition());

    }
}
