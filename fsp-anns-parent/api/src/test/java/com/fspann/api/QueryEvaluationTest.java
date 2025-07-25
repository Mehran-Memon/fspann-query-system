package com.fspann.api;

import com.fspann.common.*;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.crypto.CryptoService;
import com.fspann.key.KeyManager;
import com.fspann.key.KeyRotationPolicy;
import com.fspann.key.KeyRotationServiceImpl;
import com.fspann.query.service.QueryServiceImpl;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class QueryEvaluationTest {

    private ForwardSecureANNSystem system;

    @AfterEach
    void tearDown() throws Exception {
        if (system != null) {
            system.shutdown();
            system = null;
        }
        System.gc();
        Thread.sleep(250);
    }

    @Test
    void queryLatencyShouldBeBelow1s(@TempDir Path tempDir) throws Exception {
        system = setupSmallSystem(tempDir);
        List<QueryResult> results = system.query(new double[]{0.05, 0.05}, 1, 2);
        assertNotNull(results);
        assertFalse(results.isEmpty());

        long durationNs = ((QueryServiceImpl) system.getQueryService()).getLastQueryDurationNs();
        assertTrue(durationNs < 1_000_000_000L, "Query latency exceeded 1 second");
    }

    @Test
    void cloakedQueryShouldReturnResults(@TempDir Path tempDir) throws Exception {
        system = setupSmallSystem(tempDir);
        List<QueryResult> results = system.queryWithCloak(new double[]{0.05, 0.05}, 1, 2);
        assertNotNull(results);
        assertFalse(results.isEmpty());
    }

    @Test
    void profilerCsvShouldContainExpectedHeaders(@TempDir Path tempDir) throws Exception {
        system = setupSmallSystem(tempDir);
        Profiler profiler = system.getProfiler();

        profiler.start("mockTiming");
        Thread.sleep(10);
        profiler.stop("mockTiming");

        Path out = tempDir.resolve("profiler.csv");
        profiler.exportToCSV(out.toString());

        List<String> lines = Files.readAllLines(out);
        assertFalse(lines.isEmpty());
        assertEquals("Label,AvgTime(ms),Runs", lines.get(0));
    }

    private ForwardSecureANNSystem setupSmallSystem(Path tempDir) throws Exception {
        Path dataFile = tempDir.resolve("E:\\Research Work\\Datasets\\synthetic_data\\synthetic_128d\\synthetic_gaussian_128d_storage.csv");
        Files.writeString(dataFile, "0.0,0.0\n0.1,0.1\n1.0,1.0\n");

        Path configFile = tempDir.resolve("F:\\fspann-query-system\\fsp-anns-parent\\config\\src\\main\\resources\\config.json");
        Files.writeString(configFile, "{" +
                "\"numShards\":2," +
                "\"profilerEnabled\":true," +
                "\"opsThreshold\":2147483647," +
                "\"ageThresholdMs\":9223372036854775807}");

        Path keys = tempDir.resolve("keys.ser");
        List<Integer> dimensions = List.of(2);

        RocksDBMetadataManager metadataManager = new RocksDBMetadataManager(tempDir.toString());
        KeyManager keyManager = new KeyManager(keys.toString());
        KeyRotationPolicy policy = new KeyRotationPolicy(2, 1000);
        KeyRotationServiceImpl keyService = new KeyRotationServiceImpl(keyManager, policy, tempDir.toString(), metadataManager, null);
        CryptoService cryptoService = new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadataManager);
        keyService.setCryptoService(cryptoService);

        ForwardSecureANNSystem sys = new ForwardSecureANNSystem(
                configFile.toString(),
                dataFile.toString(),
                keys.toString(),
                dimensions,
                tempDir,
                true,
                metadataManager,
                cryptoService,
                1000
        );

        return sys;
    }
}
