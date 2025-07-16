// Adjusted test classes to match updated ForwardSecureANNSystem constructor
package com.fspann.api;

import com.fspann.common.*;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.crypto.CryptoService;
import com.fspann.key.KeyManager;
import com.fspann.key.KeyRotationPolicy;
import com.fspann.key.KeyRotationServiceImpl;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ForwardSecureANNSystemIntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(ForwardSecureANNSystemIntegrationTest.class);
    private ForwardSecureANNSystem system;

    @BeforeEach
    public void setUp() {
    }

    @AfterEach
    public void cleanup() throws InterruptedException {
        if (system != null) {
            system.shutdown();
            system = null;
        }

        // Clean up RocksDB native resources and finalize threads
        System.gc();
        Thread.sleep(250); // Ensure RocksDB native finalization completes
    }


    @AfterAll
    public static void afterAll() throws InterruptedException {
        System.gc();
        Thread.sleep(250);
    }


    @Test
    public void simpleEndToEndNearestNeighbor(@TempDir Path tempDir) throws Exception {
        Path dataFile = tempDir.resolve("data2d.csv");
        Files.writeString(dataFile, "0.0,0.0\n0.1,0.1\n1.0,1.0\n");

        Path configFile = tempDir.resolve("config.json");
        Files.writeString(configFile, "{" +
                "\"numShards\":4," +
                "\"profilerEnabled\":true," +
                "\"opsThreshold\":2147483647," +
                "\"ageThresholdMs\":9223372036854775807}");

        Path keys = tempDir.resolve("keys.ser");
        List<Integer> dimensions = Arrays.asList(2);

        RocksDBMetadataManager metadataManager = new RocksDBMetadataManager(tempDir.toString());
        KeyManager keyManager = new KeyManager(keys.toString());
        KeyRotationPolicy policy = new KeyRotationPolicy(2, 1000);
        KeyRotationServiceImpl keyService = new KeyRotationServiceImpl(keyManager, policy, tempDir.toString(), metadataManager, null);
        CryptoService cryptoService = new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadataManager);
        keyService.setCryptoService(cryptoService);

        system = new ForwardSecureANNSystem(
                configFile.toString(),
                dataFile.toString(),
                keys.toString(),
                dimensions,
                tempDir,
                false,
                metadataManager,
                cryptoService,
                1000
        );

        int indexedCount = system.getIndexedVectorCount();
        logger.info("Indexed vectors: {}", indexedCount);
        assertTrue(indexedCount > 0);

        List<QueryResult> res = system.query(new double[]{0.05, 0.05}, 1, 2);
        assertEquals(1, res.size());
    }

    @Test
    public void testVisualization(@TempDir Path tempDir) throws Exception {
        Path dataFile = tempDir.resolve("data2d.csv");
        Files.writeString(dataFile, "0.0,0.0\n0.1,0.1\n1.0,1.0\n");

        Path queryFile = tempDir.resolve("query2d.csv");
        Files.writeString(queryFile, "0.05,0.05\n");

        Path configFile = tempDir.resolve("config.json");
        Files.writeString(configFile, "{\"numShards\":4, \"profilerEnabled\":true}");

        Path keys = tempDir.resolve("keys.ser");
        List<Integer> dimensions = Arrays.asList(2);

        RocksDBMetadataManager metadataManager = new RocksDBMetadataManager(tempDir.toString());
        KeyManager keyManager = new KeyManager(keys.toString());
        KeyRotationPolicy policy = new KeyRotationPolicy(2, 1000);
        KeyRotationServiceImpl keyService = new KeyRotationServiceImpl(keyManager, policy, tempDir.toString(), metadataManager, null);
        CryptoService cryptoService = new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadataManager);
        keyService.setCryptoService(cryptoService);

        ForwardSecureANNSystem localSys = new ForwardSecureANNSystem(
                configFile.toString(),
                dataFile.toString(),
                keys.toString(),
                dimensions,
                tempDir,
                false,
                metadataManager,
                cryptoService,
                1000
        );

        int indexedCount = localSys.getIndexedVectorCount();
        assertTrue(indexedCount > 0);

        localSys.runEndToEnd(dataFile.toString(), queryFile.toString(), 1, 2);
        localSys.shutdown();
    }
}
