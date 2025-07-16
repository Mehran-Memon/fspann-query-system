package com.fspann.api;

import com.fspann.common.*;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.crypto.CryptoService;
import com.fspann.crypto.KeyUtils;
import com.fspann.index.service.SecureLSHIndexService;
import com.fspann.key.KeyManager;
import com.fspann.key.KeyRotationPolicy;
import com.fspann.key.KeyRotationServiceImpl;
import com.fspann.common.RocksDBMetadataManager;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import javax.crypto.SecretKey;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class ForwardSecurityAdversarialTest {
    private RocksDBMetadataManager metadataManager;

    @BeforeEach
    public void cleanMetadataDir(@TempDir Path tempDir) throws IOException {
        metadataManager = new RocksDBMetadataManager(tempDir.toString(), tempDir.toString());
        Path baseDir = Paths.get(metadataManager.getPointsBaseDir());

        if (Files.exists(baseDir)) {
            try (Stream<Path> files = Files.walk(baseDir)) {
                files.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(f -> {
                            if (!f.delete()) {
                                System.err.println("Failed to delete " + f.getAbsolutePath());
                            }
                        });
            }
        }
        Files.createDirectories(baseDir);
    }

    @Test
    public void testForwardSecurityAgainstKeyCompromise(@TempDir Path tempDir) throws Exception {
        System.out.println("========== Forward Security Test ==========");
        Path config = tempDir.resolve("config.json");
        Files.writeString(config, "{\"numShards\":4,\"profilerEnabled\":true,\"opsThreshold\":999999,\"ageThresholdMs\":999999}");
        Path dummyData = tempDir.resolve("dummy.csv");
        Files.writeString(dummyData, "0.0,0.0,0.0\n");
        Path keys = tempDir.resolve("keys.ser");

        KeyManager keyManager = new KeyManager(keys.toString());
        KeyRotationPolicy policy = new KeyRotationPolicy(999999, 999999);
        KeyRotationServiceImpl keyService = new KeyRotationServiceImpl(keyManager, policy, tempDir.toString(), metadataManager, null);
        CryptoService cryptoService = new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadataManager);
        keyService.setCryptoService(cryptoService);

        ForwardSecureANNSystem system = new ForwardSecureANNSystem(
                config.toString(), dummyData.toString(), keys.toString(),
                Collections.singletonList(3), tempDir, true, metadataManager, cryptoService, 1
        );
        keyService.setIndexService(system.getIndexService());

        int dim = 3;
        double[] pointBefore = {0.15, 0.15, 0.15};
        String beforeId = UUID.randomUUID().toString();
        system.insert(beforeId, pointBefore, dim);
        system.flushAll();

        SecretKey compromisedKey = KeyUtils.fromBytes(keyService.getCurrentVersion().getKey().getEncoded());
        int versionBefore = keyService.getCurrentVersion().getVersion();
        System.out.println("Compromised key (base64): " + Base64.getEncoder().encodeToString(compromisedKey.getEncoded()));

        keyService.rotateKey();
        keyService.reEncryptAll();
        ((SecureLSHIndexService) system.getIndexService()).clearCache();

        double[] pointAfter = {0.25, 0.25, 0.25};
        String afterId = UUID.randomUUID().toString();
        system.insert(afterId, pointAfter, dim);
        system.flushAll();

        // Validate versions
        EncryptedPoint beforePoint = system.getIndexService().getEncryptedPoint(beforeId);
        assertEquals(keyService.getCurrentVersion().getVersion(), beforePoint.getVersion(),
                "Pre-rotation point should have new version");

        List<QueryResult> results = system.query(pointBefore, 20, dim);
        boolean matchFound = results.stream().anyMatch(r -> r.getId().equals(beforeId));
        System.out.println("Query results:");
        results.forEach(r -> System.out.printf("Returned ID: %s | Distance: %.6f\n", r.getId(), r.getDistance()));
        assertTrue(matchFound, "Pre-compromise point should be found in the query results.");

        EncryptedPoint encryptedBefore = system.getIndexService().getEncryptedPoint(beforeId);
        assertNotNull(encryptedBefore, "Pre-rotation point should exist.");
        Optional<double[]> decryptedOld = KeyUtils.tryDecryptWithKeyOnly(encryptedBefore, compromisedKey);
        assertTrue(decryptedOld.isEmpty(), "Old key should NOT decrypt re-encrypted point");

        SecretKey currentKey = keyService.getVersion(encryptedBefore.getVersion()).getKey();
        Optional<double[]> decryptedNew = KeyUtils.tryDecryptWithKeyOnly(encryptedBefore, currentKey);
        assertTrue(decryptedNew.isPresent(), "Current key should decrypt pre-rotation point");
        metadataManager.close();
        system.shutdown();
    }
}
