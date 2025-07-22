package com.fspann.api;

import com.fspann.common.*;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.crypto.CryptoService;
import com.fspann.crypto.KeyUtils;
import com.fspann.index.service.SecureLSHIndexService;
import com.fspann.key.KeyManager;
import com.fspann.key.KeyRotationPolicy;
import com.fspann.key.KeyRotationServiceImpl;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.nio.file.*;
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

        // 1. Configuration
        Path config = tempDir.resolve("config.json");
        Files.writeString(config, "{\"numShards\":4,\"profilerEnabled\":true,\"opsThreshold\":999999,\"ageThresholdMs\":999999}");
        Path dummyData = tempDir.resolve("dummy.csv");
        Files.writeString(dummyData, "0.0,0.0,0.0\n");
        Path keys = tempDir.resolve("keys.ser");

        // 2. System init
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

        // 3. Insert pre-compromise point
        double[] pointBefore = {0.15, 0.15, 0.15};
        String beforeId = UUID.randomUUID().toString();
        system.insert(beforeId, pointBefore, dim);
        system.flushAll();

        // 4. Simulate key compromise
        SecretKey compromisedKey = KeyUtils.fromBytes(keyService.getCurrentVersion().getKey().getEncoded());
        System.out.println("Compromised key (base64): " + Base64.getEncoder().encodeToString(compromisedKey.getEncoded()));

        // 5. Key rotation and re-encryption
        keyService.rotateKey();
        keyService.reEncryptAll();
        ((SecureLSHIndexService) system.getIndexService()).clearCache();

        // 6. Insert post-compromise point
        double[] pointAfter = {0.25, 0.25, 0.25};
        String afterId = UUID.randomUUID().toString();
        system.insert(afterId, pointAfter, dim);
        system.flushAll();

        // 7. Re-validation
        EncryptedPoint beforePoint = system.getIndexService().getEncryptedPoint(beforeId);
        assertEquals(keyService.getCurrentVersion().getVersion(), beforePoint.getVersion(),
                "Pre-rotation point should have updated version");

        // 8. Query around compromised vector
        List<QueryResult> results = system.query(pointBefore, 50, dim);
        assertNotNull(results);
        assertFalse(results.isEmpty());

        // 9. Print results
        System.out.println("Query results:");
        results.forEach(r -> System.out.printf("Returned ID: %s | Distance: %.6f\n", r.getId(), r.getDistance()));

        // 10. Security checks
        Optional<double[]> decryptedOld = KeyUtils.tryDecryptWithKeyOnly(beforePoint, compromisedKey);
        assertTrue(decryptedOld.isEmpty(), "Old key must NOT decrypt re-encrypted point");

        SecretKey currentKey = keyService.getVersion(beforePoint.getVersion()).getKey();
        Optional<double[]> decryptedNew = KeyUtils.tryDecryptWithKeyOnly(beforePoint, currentKey);
        assertTrue(decryptedNew.isPresent(), "Current key must decrypt pre-rotation point");

        // 11. Precision/Recall Evaluation
        Set<String> groundtruth = Set.of(beforeId, afterId);
        long matchCount = results.stream().map(QueryResult::getId).filter(groundtruth::contains).count();

        double precision = (double) matchCount / results.size();
        double recall = (double) matchCount / groundtruth.size();

        System.out.printf("✅ Precision: %.2f, Recall: %.2f\n", precision, recall);
        assertTrue(matchCount > 0, "Expected at least one groundtruth match");
        assertTrue(precision > 0.0 && recall > 0.0, "Expected non-zero precision and recall for secure ANN");

        // 12. Shutdown
        system.shutdown();
        metadataManager.close();
    }
}
