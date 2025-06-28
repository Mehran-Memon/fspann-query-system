package com.fspann.api;

import com.fspann.common.EncryptedPoint;
import com.fspann.common.MetadataManager;
import com.fspann.common.QueryResult;
import com.fspann.crypto.AesGcmCryptoService;
import com.fspann.crypto.CryptoService;
import com.fspann.crypto.KeyUtils;
import com.fspann.key.KeyManager;
import com.fspann.key.KeyRotationPolicy;
import com.fspann.key.KeyRotationServiceImpl;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.crypto.SecretKey;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class ForwardSecurityAdversarialTest {

    @Test
    public void testForwardSecurityAgainstKeyCompromise(@TempDir Path tempDir) throws Exception {
        System.out.println("========== Forward Security Test ==========");

        Path config = tempDir.resolve("config.json");
        Files.writeString(config, "{" +
                "\"numShards\":4," +
                "\"profilerEnabled\":true," +
                "\"opsThreshold\":2," +
                "\"ageThresholdMs\":1000}");

        Path dummyData = tempDir.resolve("dummy.csv");
        Files.writeString(dummyData, "0.0,0.0,0.0\n");

        Path keys = tempDir.resolve("keys.ser");
        List<Integer> dimensions = Collections.singletonList(3);

        MetadataManager metadataManager = new MetadataManager();
        KeyManager keyManager = new KeyManager(keys.toString());
        KeyRotationPolicy policy = new KeyRotationPolicy(2, 1000);
        KeyRotationServiceImpl keyService = new KeyRotationServiceImpl(keyManager, policy, tempDir.toString(), metadataManager, null);
        CryptoService cryptoService = new AesGcmCryptoService(new SimpleMeterRegistry(), keyService, metadataManager);
        keyService.setCryptoService(cryptoService);

        ForwardSecureANNSystem system = new ForwardSecureANNSystem(
                config.toString(),
                dummyData.toString(),
                keys.toString(),
                dimensions,
                tempDir,
                true,
                metadataManager,
                cryptoService
        );

        int dim = 3;

        // Insert nearby distractors
        for (int i = 0; i < 10; i++) {
            double offset = 0.001 * i;
            double[] closeVec = new double[]{0.15 + offset, 0.15 - offset, 0.15};
            system.insert(UUID.randomUUID().toString(), closeVec, dim);
        }

        // Insert pre-rotation point
        double[] pointBefore = new double[]{0.15, 0.15, 0.15};
        String beforeId = UUID.randomUUID().toString();
        system.insert(beforeId, pointBefore, dim);

        SecretKey compromisedKey = KeyUtils.fromBytes(
                system.getCryptoService().getKeyService().getCurrentVersion().getKey().getEncoded()
        );
        int versionBefore = system.getCryptoService().getKeyService().getCurrentVersion().getVersion();
        System.out.println("[!] Compromised key (base64): " + Base64.getEncoder().encodeToString(compromisedKey.getEncoded()));

        // Trigger rotation
        for (int i = 0; i < 20; i++) {
            double[] dummy = new double[]{Math.random(), Math.random(), Math.random()};
            system.insert(UUID.randomUUID().toString(), dummy, dim);
        }

        int versionAfter = system.getCryptoService().getKeyService().getCurrentVersion().getVersion();
        System.out.printf("[!] Key Version Before: %d, After: %d\n", versionBefore, versionAfter);
        assertTrue(versionAfter > versionBefore, "Key should have rotated");

        // Insert post-rotation point
        double[] pointAfter = new double[]{0.25, 0.25, 0.25};
        String afterId = UUID.randomUUID().toString();
        system.insert(afterId, pointAfter, dim);

        // Query to see if pre-rotation point is still findable
        List<QueryResult> results = system.query(pointBefore, 20, dim);
        boolean matchFound = results.stream().anyMatch(r -> r.getId().equals(beforeId));
        System.out.println("[!] Query results:");
        results.forEach(r -> System.out.printf("Returned ID: %s | Distance: %.6f\n", r.getId(), r.getDistance()));
        if (!matchFound) {
            System.out.println("[!] ANN did not retrieve the pre-compromise point. This is acceptable under approximation.");
        }

        // Try to decrypt both points
        EncryptedPoint encryptedBefore = system.getEncryptedPointById(beforeId);
        assertNotNull(encryptedBefore);
        Optional<double[]> decryptedOld = KeyUtils.tryDecryptWithKeyOnly(encryptedBefore, compromisedKey);
        System.out.println("[?] Decryption of pre-rotation point with compromised key: " + (decryptedOld.isEmpty() ? "✔ BLOCKED" : "❌ FAILED"));
        assertTrue(decryptedOld.isEmpty(), "Old key should NOT decrypt pre-rotation point (re-encrypted after rotation)");

        EncryptedPoint encryptedAfter = system.getEncryptedPointById(afterId);
        assertNotNull(encryptedAfter);
        SecretKey currentKey = system.getCryptoService().getKeyService().getCurrentVersion().getKey();
        Optional<double[]> decryptedNew = KeyUtils.tryDecryptWithKeyOnly(encryptedAfter, currentKey);
        System.out.println("[+] Decryption of post-rotation point with current key: " + (decryptedNew.isPresent() ? "✔ SUCCESS" : "❌ FAILED"));
        assertTrue(decryptedNew.isPresent(), "Current key should decrypt post-rotation point");

        System.out.println("========= ✅ FORWARD SECURITY VALIDATED =========");
        system.shutdown();
    }
}
